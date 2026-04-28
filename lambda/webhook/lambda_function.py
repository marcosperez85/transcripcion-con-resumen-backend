import json
import hmac
import hashlib
import os
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
usage_table = dynamodb.Table(os.environ["USAGE_TABLE"])
secrets_client = boto3.client("secretsmanager")

SECRET_ARN = os.environ["SECRET_ARN"]
_lemon_secret = None

def get_secret():
    global _lemon_secret
    if _lemon_secret is None:
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        _lemon_secret = response["SecretString"]
    return _lemon_secret

def lambda_handler(event, context):
    try:
        # Extraemos el body crudo (necesario para verificar la firma de Lemon Squeezy)
        raw_body = event.get("body", "")
        if not raw_body:
            return {"statusCode": 400, "body": "Empty body"}

        headers = event.get("headers", {})
        # API Gateway puede convertir los headers a minúsculas
        signature = headers.get("X-Signature") or headers.get("x-signature")

        if not signature:
            logger.error("Missing X-Signature header")
            return {"statusCode": 401, "body": "Missing signature"}

        secret = get_secret()

        # Calcular HMAC SHA256 con el secret y el body crudo
        computed_signature = hmac.new(
            secret.encode("utf-8"),
            raw_body.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

        # Usar compare_digest para prevenir ataques de timing
        if not hmac.compare_digest(computed_signature, signature):
            logger.error("Invalid signature")
            return {"statusCode": 401, "body": "Invalid signature"}

        # Parsear el payload JSON
        payload = json.loads(raw_body)
        meta = payload.get("meta", {})
        event_name = meta.get("event_name")
        custom_data = meta.get("custom_data", {})
        
        # El frontend DEBE enviar user_id dentro de custom_data al crear el checkout
        user_id = custom_data.get("user_id")

        logger.info(f"Received event: {event_name} for user: {user_id}")

        if not user_id:
            logger.warning("No user_id found in custom_data. Ignoring.")
            return {"statusCode": 200, "body": "Ignored: No user_id"}

        # ===== LÓGICA DE NEGOCIO =====
        # Define cuántos segundos agregar por pago. Ejemplo: 10 horas = 36000 segundos.
        # En producción, podrías revisar payload["data"]["attributes"]["variant_id"] para saber qué plan compró.
        EXTRA_SECONDS = 36000

        if event_name in ["order_created", "subscription_created", "subscription_payment_success"]:
            logger.info(f"Acreditando {EXTRA_SECONDS} segundos al usuario {user_id}")
            # ADD suma al valor existente o crea el item si no existe
            usage_table.update_item(
                Key={"userId": user_id},
                UpdateExpression="ADD limitSeconds :increment",
                ExpressionAttributeValues={":increment": EXTRA_SECONDS}
            )
        elif event_name in ["subscription_cancelled", "subscription_expired", "subscription_payment_failed"]:
            # Aquí podrías reducir el límite o revocar acceso
            logger.info(f"Evento {event_name} procesado para {user_id}. No se tomaron acciones de resta.")
            pass

        return {"statusCode": 200, "body": "Success"}

    except Exception as e:
        logger.error(f"Error procesando webhook: {str(e)}")
        return {"statusCode": 500, "body": "Internal server error"}
