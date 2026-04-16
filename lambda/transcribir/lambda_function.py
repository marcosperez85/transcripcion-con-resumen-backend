import boto3
import uuid
import logging
import json
import os
import base64
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

transcribe_client = boto3.client("transcribe")
s3_client = boto3.client("s3")

output_bucket = os.environ["BUCKET"]

dynamodb = boto3.resource("dynamodb")
usage_table = dynamodb.Table(os.environ["USAGE_TABLE"])


# -----------------------------------------------------
# Helpers
# -----------------------------------------------------

def response(code, payload):
    return {
        "statusCode": code,
        "headers": {
            "Access-Control-Allow-Origin": "https://d11ahn26gyfe9q.cloudfront.net",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        },
        "body": json.dumps(payload),
    }


def parse_body(event):
    if "body" not in event:
        return event

    body = event["body"]

    if isinstance(body, str):
        body = json.loads(body)

    if isinstance(body, str):
        body = json.loads(body)

    return body


def get_user_sub(event):
    try:
        return event["requestContext"]["authorizer"]["claims"]["sub"]
    except Exception:
        raise Exception("Missing authorizer claims")

def check_usage_limit(user_id):

    resp = usage_table.get_item(Key={"userId": user_id})

    if "Item" not in resp:
        return False, 0, 1800

    item = resp["Item"]

    used = item.get("totalSeconds", 0)
    limit = item.get("limitSeconds", 1800)

    return used >= limit, used, limit


def object_exists(bucket, key):

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


# -----------------------------------------------------
# Lambda handler
# -----------------------------------------------------

def lambda_handler(event, context):

    logger.info("Event received")
    logger.info(json.dumps(event))

    try:

        body = parse_body(event)

        # ALWAYS extract user first
        try:
            user_id = get_user_sub(event)
        except Exception as e:
            logger.error(f"Auth error: {str(e)}")
            return response(401, {"error": "Unauthorized"})
        
        supported_formats = [
            "amr", "flac", "m4a", "mp3", "mp4", "ogg", "webm", "wav"
        ]

        # -------------------------------------------------
        # ROUTE: checkStatus
        # -------------------------------------------------
        if "checkStatus" in body:

            job_name = body["checkStatus"]["job_name"]

            tj = transcribe_client.get_transcription_job(
                TranscriptionJobName=job_name
            )

            status = tj["TranscriptionJob"]["TranscriptionJobStatus"]

            formatted_key = f"transcripciones-formateadas/{user_id}/{job_name}.txt"
            summary_key = f"resumenes/{user_id}/{job_name}_summary.txt"

            return response(
                200,
                {
                    "status": status,
                    "formattedReady": object_exists(output_bucket, formatted_key),
                    "summaryReady": object_exists(output_bucket, summary_key),
                    "keys": {
                        "formatted": formatted_key,
                        "summary": summary_key,
                    },
                },
            )

        # -------------------------------------------------
        # ROUTE: getResults
        # -------------------------------------------------
        if "getResults" in body:

            job_name = body["getResults"]["job_name"]

            formatted_key = f"transcripciones-formateadas/{user_id}/{job_name}.txt"
            summary_key = f"resumenes/{user_id}/{job_name}_summary.txt"

            transcription = None
            summary = None

            try:
                obj = s3_client.get_object(Bucket=output_bucket, Key=formatted_key)
                transcription = obj["Body"].read().decode("utf-8")
            except ClientError:
                pass

            try:
                obj = s3_client.get_object(Bucket=output_bucket, Key=summary_key)
                summary = obj["Body"].read().decode("utf-8")
            except ClientError:
                pass

            return response(
                200,
                {
                    "transcription": transcription,
                    "summary": summary,
                },
            )

        # -------------------------------------------------
        # ROUTE: start transcription
        # -------------------------------------------------

        exceeded, used, limit = check_usage_limit(user_id)

        if exceeded:
            return response(
                403,
                {
                    "error": "Usage limit reached",
                    "usedSeconds": used,
                    "limitSeconds": limit,
                },
            )

        s3_info = body.get("s3")

        if not s3_info:
            return response(400, {"error": "Missing s3 info"})

        key = s3_info["key"]

        # Validar que la extensión corresponda a un audio pero sin limitarse a mp3
        if not any(key.lower().endswith(f".{fmt}") for fmt in supported_formats) or not key.startswith("audios/"):
            return response(400, {"error": "Invalid S3 key or unsupported format"})

        # -------------------------------------------------
        # check audio size
        # -------------------------------------------------

        obj = s3_client.head_object(Bucket=output_bucket, Key=key)

        size_bytes = obj["ContentLength"]

        size_mb = size_bytes / (1024 * 1024)

        MAX_AUDIO_MB = 30

        logger.info(f"USAGE → used: {used}, limit: {limit}")

        if size_mb > MAX_AUDIO_MB:
            return response(
                400,
                {
                    "error": "Audio too long for beta users",
                    "maxMinutes": 30,
                },
            )
        # -------------------------------------------------
        # Revisar formato de audio
        # -------------------------------------------------

        file_key = body["s3"]["key"]
        media_format = file_key.split(".")[-1].lower()

        # -------------------------------------------------
        # start transcription job
        # -------------------------------------------------

        language_code = body["transcribe"]["languageCode"]
        max_speakers = body["transcribe"].get("maxSpeakers", 2)

        settings = {}

        # SOLO habilitar speaker labels si maxSpeakers > 1
        if isinstance(max_speakers, int) and max_speakers > 1:
            settings["ShowSpeakerLabels"] = True
            settings["MaxSpeakerLabels"] = max_speakers


        job_name = f"{user_id}-{uuid.uuid4()}"

        # Usar file_key en lugar de key para consistencia
        media_uri = f"s3://{output_bucket}/{file_key}"

        output_key = f"transcripciones/{user_id}/{job_name}.json"

        transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={"MediaFileUri": media_uri},
            MediaFormat=media_format,
            LanguageCode=language_code,
            OutputBucketName=output_bucket,
            OutputKey=output_key,
            Settings=settings
        )

        logger.info(f"Transcription started: {media_uri}")

        return response(
            200,
            {
                "message": "Transcription started",
                "jobName": job_name,
                "outputLocation": f"s3://{output_bucket}/{output_key}",
            },
        )

    except Exception as e:

        logger.error(str(e))
        return response(500, {"error": str(e)})