import boto3
import uuid
import logging
import json
import os
import base64
import subprocess
import tempfile
from datetime import datetime
from decimal import Decimal
from botocore.exceptions import ClientError

# Clase auxiliar para serializar objetos Decimal de DynamoDB
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# -----------------------------------------------------
# Variables Globales
# -----------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

transcribe_client = boto3.client("transcribe")
s3_client = boto3.client("s3")

output_bucket = os.environ["BUCKET"]

dynamodb = boto3.resource("dynamodb")
usage_table = dynamodb.Table(os.environ["USAGE_TABLE"])

# Modo de prueba (para evitar usar Amazon Transcribe)
TEST_MODE = os.environ.get("TEST_MODE", "false").lower() == "true"

# Límite de segundos gratuitos
segundos_gratis = 600
max_minutes_audio = 10


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
        "body": json.dumps(payload, cls=DecimalEncoder),
    }


def parse_body(event):
    if "body" not in event:
        return event

    body = event["body"]

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
        return False, 0, segundos_gratis

    item = resp["Item"]

    used = item.get("totalSeconds", 0)
    limit = item.get("limitSeconds", segundos_gratis)

    return used >= limit, used, limit


def object_exists(bucket, key):

    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

def needs_conversion(media_format):
    """Check if audio format needs conversion"""
    # Formats that often contain unsupported codecs
    problematic_formats = ["m4a", "mp4", "ogg", "webm"]
    return media_format in problematic_formats

def convert_to_mp3(input_key):
    """Convert audio file to MP3 format"""
    logger.info(f"Converting {input_key} to MP3")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        ext = input_key.split('.')[-1]
        input_file = os.path.join(temp_dir, f"input_audio.{ext}")
        output_file = os.path.join(temp_dir, "output.mp3")
        
        # Download original file
        s3_client.download_file(output_bucket, input_key, input_file)
        
        # Convert using FFmpeg (simple, reliable settings)
        try:
            subprocess.run([
                '/opt/bin/ffmpeg',
                '-i', input_file,
                '-acodec', 'libmp3lame',  # MP3 codec
                '-ar', '16000',           # Good for speech recognition
                '-ac', '1',               # Mono (saves bandwidth)
                '-b:a', '64k',           # Sufficient for speech
                '-y',                    # Overwrite output file
                output_file
            ], check=True, capture_output=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg failed: {e.stderr}")
            raise
        
        # Upload converted file
        converted_key = input_key.replace(f".{input_key.split('.')[-1]}", "_converted.mp3")
        s3_client.upload_file(output_file, output_bucket, converted_key)
        
        logger.info(f"Conversion complete: {converted_key}")
        return converted_key


def estimate_audio_duration(size_bytes, format_type):
    """
    Estimate audio duration based on file size and format.
    This is a fallback method when we can't directly analyze the audio.
    """
    # Average bitrates for different formats (in bits per second)
    bitrates = {
        "mp3": 128000,  # 128 kbps
        "wav": 1411000,  # CD quality
        "flac": 700000,  # ~700 kbps
        "ogg": 128000,  # ~128 kbps
        "m4a": 128000,  # ~128 kbps
        "amr": 12200,   # ~12.2 kbps
        "mp4": 128000,  # audio track estimate
        "webm": 128000  # audio track estimate
    }
    
    bitrate = bitrates.get(format_type.lower(), 128000)
    
    # Duration = size in bits / bitrate
    estimated_duration = (size_bytes * 8) / bitrate
    
    # Add 10% buffer to the estimate
    return int(estimated_duration * 1.1)


def update_usage_record(user_id, audio_duration_seconds):
    """
    Updates the user's usage record in DynamoDB by adding the audio duration
    to their total used seconds.
    
    Args:
        user_id: The Cognito user ID (sub)
        audio_duration_seconds: Duration of processed audio in seconds
    """
    try:
        # Use UpdateExpression to either create a new record or update existing one
        response = usage_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="""
                SET totalSeconds = if_not_exists(totalSeconds, :zero) + :duration,
                limitSeconds = if_not_exists(limitSeconds, :limit),
                updatedAt = :now
            """,
            ExpressionAttributeValues={
                ":duration": audio_duration_seconds,
                ":zero": 0,
                ":limit": segundos_gratis,
                ":now": datetime.utcnow().isoformat()
            },
            ReturnValues="UPDATED_NEW"
        )
        
        logger.info(f"Updated usage for user {user_id}: +{audio_duration_seconds}s, " 
                   f"new total: {response.get('Attributes', {}).get('totalSeconds', 'unknown')}s")
        return True
    except Exception as e:
        logger.error(f"Error updating usage record: {str(e)}")
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
            logger.info({
                "path": event.get("path"),
                "user": user_id
            })
        except Exception as e:
            logger.error(f"Auth error: {str(e)}")
            return response(401, {"error": "Unauthorized"})
        
        supported_formats = [
            "amr", "flac", "m4a", "mp3", "mp4", "ogg", "webm", "wav"
        ]

        # -------------------------------------------------
        # ROUTE: checkUsage
        # -------------------------------------------------
        if "checkUsage" in body:
            resp = usage_table.get_item(Key={"userId": user_id})

            used_seconds = 0
            limit_seconds = segundos_gratis
            
            if "Item" in resp:
                item = resp["Item"]
                used_seconds = item.get("totalSeconds", 0)
                limit_seconds = item.get("limitSeconds", segundos_gratis)
            
            remaining_seconds = max(0, limit_seconds - used_seconds)
            
            return response(
                200,
                {
                    "usedSeconds": used_seconds,
                    "limitSeconds": limit_seconds,
                    "remainingSeconds": remaining_seconds
                },
            )

        # -------------------------------------------------
        # ROUTE: checkStatus
        # -------------------------------------------------
        if "checkStatus" in body:

            job_name = body["checkStatus"]["job_name"]

            # Consultar estado de transcripción desde Amazon Transcribe
            tj = transcribe_client.get_transcription_job(
                TranscriptionJobName=job_name
            )

            status = tj["TranscriptionJob"]["TranscriptionJobStatus"]

            # Consultar estado del job desde DynamoDB en lugar de verificar S3
            resp = usage_table.get_item(Key={"userId": user_id})
            job_status = resp.get("Item", {}).get("jobStatus", "PROCESSING")

            # Mantener las keys para compatibilidad con frontend existente
            formatted_key = f"transcripciones-formateadas/{user_id}/{job_name}.txt"
            summary_key = f"resumenes/{user_id}/{job_name}_summary.txt"

            return response(
                200,
                {
                    "status": status,
                    "jobStatus": job_status,
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
                    "maxMinutes": max_minutes_audio,
                },
            )
        # -------------------------------------------------
        # Estimate audio duration and update DynamoDB
        # -------------------------------------------------
        # Estimate audio duration based on file size and format
        original_format = key.split(".")[-1].lower()
        estimated_duration = estimate_audio_duration(size_bytes, original_format)
        logger.info(f"Estimated duration of audio: {estimated_duration} seconds")

        # Crear el job_name ANTES de usarlo en la actualización de DynamoDB
        job_name = f"{user_id}-{uuid.uuid4()}"
        logger.info(f"Generated job name: {job_name}")
        
        # Update usage tracking in DynamoDB - add pendingSeconds
        usage_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="""
                SET pendingSeconds = if_not_exists(pendingSeconds, :zero) + :duration,
                    jobStatus = :status,
                    lastJobId = :jobId
            """,
            ExpressionAttributeValues={
                ":duration": estimated_duration,
                ":zero": 0,
                ":status": "PROCESSING",
                ":jobId": job_name
            }
        )
            
        # -------------------------------------------------
        # Revisar formato de audio y realizar conversión si fuera necesario
        # -------------------------------------------------

        # file_key = body["s3"]["key"]
        # original_format = file_key.split(".")[-1].lower()

        media_format = original_format
        file_key = key

        # Convert if needed
        if needs_conversion(original_format):
            file_key = convert_to_mp3(file_key)
            media_format = "mp3"
        else:
            media_format = original_format

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

        # Usar file_key en lugar de key para consistencia
        media_uri = f"s3://{output_bucket}/{file_key}"

        output_key = f"transcripciones/{user_id}/{job_name}.json"

        # usage_table.update_item(
        #     Key={"userId": user_id},
        #     UpdateExpression="""
        #         SET lastJobId = :job,
        #             jobStatus = :status
        #     """,
        #     ExpressionAttributeValues={
        #         ":job": job_name,
        #         ":status": "PROCESSING"
        #     }
        # )
        
        # Verificar si estamos en modo de prueba (para evitar costos)
        # Solo se activa con la variable de entorno TEST_MODE=true
        if TEST_MODE:
            # En modo de prueba, simulamos una transcripción exitosa
            logger.info("TEST MODE: Simulando transcripción exitosa sin usar Amazon Transcribe")
            
            # Crear una transcripción de prueba y subirla a S3
            sample_transcript = {
                "jobName": job_name,
                "accountId": "123456789012",
                "results": {
                    "transcripts": [
                        {"transcript": "Esta es una transcripción de prueba para evitar costos de Amazon Transcribe."}
                    ],
                    "items": [
                        {"start_time": "0.0", "end_time": "2.0", "alternatives": [{"content": "Esta"}], "type": "pronunciation"},
                        {"start_time": "2.1", "end_time": "2.2", "alternatives": [{"content": "es"}], "type": "pronunciation"},
                        {"start_time": "2.3", "end_time": "2.4", "alternatives": [{"content": "una"}], "type": "pronunciation"},
                        {"start_time": "2.5", "end_time": "3.0", "alternatives": [{"content": "transcripción"}], "type": "pronunciation"},
                        {"start_time": "3.1", "end_time": "3.2", "alternatives": [{"content": "de"}], "type": "pronunciation"},
                        {"start_time": "3.3", "end_time": "3.5", "alternatives": [{"content": "prueba"}], "type": "pronunciation"},
                        {"start_time": "3.6", "end_time": "3.8", "alternatives": [{"content": "para"}], "type": "pronunciation"},
                        {"start_time": "3.9", "end_time": "4.1", "alternatives": [{"content": "evitar"}], "type": "pronunciation"},
                        {"start_time": "4.2", "end_time": "4.5", "alternatives": [{"content": "costos"}], "type": "pronunciation"},
                        {"start_time": "4.6", "end_time": "4.8", "alternatives": [{"content": "de"}], "type": "pronunciation"},
                        {"start_time": "4.9", "end_time": "5.2", "alternatives": [{"content": "Amazon"}], "type": "pronunciation"},
                        {"start_time": "5.3", "end_time": "6.0", "alternatives": [{"content": "Transcribe"}], "type": "pronunciation"},
                        {"start_time": "6.1", "end_time": "6.2", "alternatives": [{"content": "."}], "type": "punctuation"}
                    ],
                    "speaker_labels": {
                        "speakers": 1,
                        "segments": [
                            {
                                "start_time": "0.0",
                                "end_time": "6.0",
                                "speaker_label": "spk_0",
                                "items": [
                                    {"start_time": "0.0", "speaker_label": "spk_0"},
                                    {"start_time": "2.1", "speaker_label": "spk_0"},
                                    {"start_time": "2.3", "speaker_label": "spk_0"},
                                    {"start_time": "2.5", "speaker_label": "spk_0"},
                                    {"start_time": "3.1", "speaker_label": "spk_0"},
                                    {"start_time": "3.3", "speaker_label": "spk_0"},
                                    {"start_time": "3.6", "speaker_label": "spk_0"},
                                    {"start_time": "3.9", "speaker_label": "spk_0"},
                                    {"start_time": "4.2", "speaker_label": "spk_0"},
                                    {"start_time": "4.6", "speaker_label": "spk_0"},
                                    {"start_time": "4.9", "speaker_label": "spk_0"},
                                    {"start_time": "5.3", "speaker_label": "spk_0"}
                                ]
                            }
                        ]
                    }
                },
                "status": "COMPLETED"
            }
            
            # Subir la transcripción de prueba a S3
            s3_client.put_object(
                Bucket=output_bucket,
                Key=output_key,
                Body=json.dumps(sample_transcript, cls=DecimalEncoder).encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"TEST MODE: Transcripción de prueba guardada en: s3://{output_bucket}/{output_key}")
            
            # Añadir un delay mínimo para simular procesamiento
            import time
            time.sleep(1)
        else:
            # Modo normal: usar Amazon Transcribe
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
                "estimatedDuration": estimated_duration
            },
        )

    except Exception as e:

        logger.error(str(e))
        return response(500, {"error": str(e)})