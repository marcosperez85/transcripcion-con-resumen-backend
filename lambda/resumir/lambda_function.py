import json
import boto3
import os
import logging
from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OUTPUT_BUCKET = os.environ["BUCKET"]
REGION = os.environ["AWS_REGION"]

# Añadir la definición de TEST_MODE
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"

s3 = boto3.client("s3")
bedrock = boto3.client(
    "bedrock-runtime",
    region_name=REGION
)

dynamodb = boto3.resource("dynamodb")
usage_table = dynamodb.Table(os.environ["USAGE_TABLE"])

# Cambiado a Claude 3 Sonnet para mejor soporte multilingüe
MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

def update_final_usage(user_id):
    """
    Actualiza el uso final al completar la transcripción.
    Mueve la duración pendiente al total de segundos usados.
    """
    try:
        # Obtener los segundos pendientes actuales (duración real calculada en formatear)
        resp = usage_table.get_item(Key={"userId": user_id})
        
        if "Item" not in resp or "pendingSeconds" not in resp["Item"]:
            logger.warning(f"No pending seconds found for user {user_id}")
            return False
            
        pending_seconds = resp["Item"]["pendingSeconds"]
        
        if not pending_seconds:
            logger.warning(f"Zero pending seconds for user {user_id}")
            return False
            
        # Actualizar totalSeconds y limpiar pendingSeconds
        usage_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="""
                ADD totalSeconds :duration
                SET pendingSeconds = :zero,
                    updatedAt = :now
            """,
            ExpressionAttributeValues={
                ":duration": pending_seconds,
                ":zero": 0,
                ":now": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Updated final usage for user {user_id}: +{pending_seconds}s")
        return True
    except Exception as e:
        logger.error(f"Error updating final usage: {str(e)}")
        return False

def lambda_handler(event, context):
    key = None

    try:
        # ---- Input desde S3 ----
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]

        # key example: transcripciones-formateadas/{user_id}/{file}.txt
        user_id = key.split("/")[1]
        file_name = os.path.basename(key)

        logger.info(f"Procesando archivo: s3://{bucket}/{key}")

        response = s3.get_object(Bucket=bucket, Key=key)
        text = response["Body"].read().decode("utf-8")

        # ---- Prompt recomendado ----
        MAX_CHARS = 15000

        if len(text) > MAX_CHARS:
            logger.warning(f"Text too long ({len(text)}), truncating...")
            text = text[:MAX_CHARS]
            
        prompt = f"""
You are a professional summarization assistant who respects the original language of the text.

TASK:
Generate a clean, well-structured summary from this transcription

LANGUAGE INSTRUCTION:
- If the text is in Spanish: Debes responder COMPLETAMENTE en español.
- If the text is in English: Respond completely in English.
- For any other language: Maintain that original language.

REQUIREMENTS:
- Output ONLY the summary.
- Do NOT repeat sentences from the original text.
- Do NOT include separators, tables, or special characters.
- Use a concise bullet list.
- Do NOT add any additional comments

TEXT START
{text}
TEXT END
""".strip()

        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1024,
            "temperature": 0.3,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }

        if not text.strip():
            raise Exception("Empty transcription")

        if TEST_MODE:
            logger.info("TEST MODE: Generando resumen mock")

            summary = """
        - Este es un resumen de prueba
        - Generado sin usar Bedrock
        - Sirve para testear la UI
        """.strip()

        else:
            response = bedrock.invoke_model(
                modelId=MODEL_ID,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body)
            )

            response_body = json.loads(response["body"].read())
            summary = response_body["content"][0]["text"]

        # ---- Output ----
        filename = os.path.basename(key)
        summary_key = f"resumenes/{user_id}/{filename.replace('.txt', '_summary.txt')}"

        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=summary_key,
            Body=summary.encode("utf-8")
        )

        update_final_usage(user_id)

        # Actualizar estado del job a DONE
        usage_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="""
                SET jobStatus = :status
            """,
            ExpressionAttributeValues={
                ":status": "DONE"
            }
        )
        
        logger.info(f"Se utilizó el modelo: {MODEL_ID}")
        logger.info(f"Resumen generado: s3://{OUTPUT_BUCKET}/{summary_key}")
        logger.info(f"USER {user_id} summary generated for {file_name}")

        return {
            "status": "COMPLETED",
            "output": summary_key
        }

    # ---- Manejo explícito de errores Bedrock ----
    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        logger.error(f"Error Bedrock: {error_code} - {str(e)}")

        error_payload = {
            "status": "FAILED",
            "error": "BEDROCK_MODEL_ERROR",
            "detail": error_code
        }

        _write_failed_status(key, error_payload)
        return error_payload

    # ---- Error genérico ----
    except Exception as e:
        logger.exception("Error inesperado en Lambda")

        error_payload = {
            "status": "FAILED",
            "error": "UNEXPECTED_ERROR",
            "detail": str(e)
        }

        _write_failed_status(key, error_payload)
        return error_payload


def _write_failed_status(input_key, payload):
    """
    Escribe un archivo FAILED para que el frontend
    pueda cortar el polling inmediatamente.
    """
    try:
        parts = input_key.split("/")
        if len(parts) >= 3:
            user_id = parts[1]
            filename = parts[-1]
            error_key = f"resumenes/{user_id}/{filename}_FAILED.json"

            s3.put_object(
                Bucket=OUTPUT_BUCKET,
                Key=error_key,
                Body=json.dumps(payload).encode("utf-8")
            )

            # En caso de fallo, también actualizamos DynamoDB
            usage_table.update_item(
                Key={"userId": user_id},
                UpdateExpression="SET jobStatus = :status",
                ExpressionAttributeValues={":status": "FAILED"}
            )

            logger.info(f"Estado FAILED escrito en s3://{OUTPUT_BUCKET}/{error_key}")
    except Exception as e:
        logger.error(f"Error escribiendo estado fallido: {str(e)}")
