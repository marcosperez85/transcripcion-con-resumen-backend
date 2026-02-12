import json
import boto3
import os
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

OUTPUT_BUCKET = os.environ["BUCKET"]
REGION = os.environ["AWS_REGION"]

s3 = boto3.client("s3")
bedrock = boto3.client(
    "bedrock-runtime",
    region_name=REGION
)

MODEL_ID = "meta.llama3-8b-instruct-v1:0"


def lambda_handler(event, context):
    key = None

    try:
        # ---- Input desde S3 ----
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        key = event["Records"][0]["s3"]["object"]["key"]

        logger.info(f"Procesando archivo: s3://{bucket}/{key}")

        response = s3.get_object(Bucket=bucket, Key=key)
        text = response["Body"].read().decode("utf-8")

        # ---- Prompt recomendado ----
        prompt = f"""
            You are a professional summarization assistant.

            TASK:
            Generate a clean, well-structured summary.

            REQUIREMENTS:
            - Output ONLY the summary.
            - Do NOT repeat sentences from the original text.
            - Do NOT include separators, tables, or special characters.
            - Use a concise bullet list.
            - Preserve the original language.

            TEXT START
            {text}
            TEXT END

            SUMMARY:
            """.strip()

        body = {
            "prompt": prompt,
            "max_gen_len": 1024,
            "temperature": 0.3,
            "top_p": 0.9
        }

        # ---- Invocación a Bedrock ----
        response = bedrock.invoke_model(
            modelId=MODEL_ID,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body)
        )

        response_body = json.loads(response["body"].read())

        # ---- Parsing correcto ----
        summary = response_body["generation"]

        # ---- Output ----
        filename = os.path.basename(key)
        summary_key = f"resumenes/{filename.replace('.txt', '_summary.txt')}"

        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=summary_key,
            Body=summary.encode("utf-8")
        )

        logger.info(f"Resumen generado: s3://{OUTPUT_BUCKET}/{summary_key}")

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
    if not input_key:
        return

    filename = os.path.basename(input_key)
    error_key = f"resumenes/{filename}_FAILED.json"

    s3.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=error_key,
        Body=json.dumps(payload).encode("utf-8")
    )

    logger.info(f"Estado FAILED escrito en s3://{OUTPUT_BUCKET}/{error_key}")
