import boto3
import uuid
import logging
import json
import os
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

transcribe_client = boto3.client('transcribe')
s3_client = boto3.client('s3')
output_bucket = os.environ['BUCKET']

def _resp(status_code, payload_dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(payload_dict),
    }

def _object_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        # Si no existe, devolvemos False; para otros errores, también False
        return False

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")

    # Normalizamos body
    if 'body' in event:
        try:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        except Exception as e:
            logger.error(f"Error parsing body: {str(e)}")
            return _resp(400, {"error": "Invalid body"})
    else:
        body = event

    logger.info(f"Request body: {body}")

    # ---------------------------
    # RUTA 1: checkStatus
    # ---------------------------
    if 'checkStatus' in body:
        try:
            job_name = body['checkStatus']['job_name']

            # Estado del job de Transcribe
            tj = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
            status = tj['TranscriptionJob']['TranscriptionJobStatus']  # IN_PROGRESS | COMPLETED | FAILED

            # Claves esperadas por tu pipeline por eventos:
            #   transcripciones/JobName.json          (lo escribe Transcribe)
            #   transcripciones-formateadas/JobName.txt (lo escribe tu Lambda de "formatear")
            #   resumenes/JobName_summary.txt           (lo escribe tu Lambda de "resumir")
            formatted_key = f"transcripciones-formateadas/{job_name}.txt"
            summary_key   = f"resumenes/{job_name}_summary.txt"

            formatted_ready = _object_exists(output_bucket, formatted_key)
            summary_ready   = _object_exists(output_bucket, summary_key)

            return _resp(200, {
                "status": status,
                "formattedReady": formatted_ready,
                "summaryReady": summary_ready,
                "keys": {
                    "formatted": formatted_key,
                    "summary": summary_key
                }
            })
        except Exception as e:
            logger.error(f"checkStatus error: {str(e)}")
            return _resp(500, {"error": str(e)})

    # ---------------------------
    # RUTA 2: getResults
    # ---------------------------
    if 'getResults' in body:
        try:
            job_name = body['getResults']['job_name']
            # bucketName viene en body pero usamos el oficial del stack por env
            formatted_key = f"transcripciones-formateadas/{job_name}.txt"
            summary_key   = f"resumenes/{job_name}_summary.txt"

            transcription = None
            summary = None

            try:
                obj = s3_client.get_object(Bucket=output_bucket, Key=formatted_key)
                transcription = obj['Body'].read().decode('utf-8')
            except ClientError:
                pass

            try:
                obj = s3_client.get_object(Bucket=output_bucket, Key=summary_key)
                summary = obj['Body'].read().decode('utf-8')
            except ClientError:
                pass

            return _resp(200, {
                "transcription": transcription,
                "summary": summary
            })
        except Exception as e:
            logger.error(f"getResults error: {str(e)}")
            return _resp(500, {"error": str(e)})

    # ---------------------------
    # RUTA 3: iniciar transcripción (comportamiento original)
    # ---------------------------
    try:
        bucketName = body['s3']['bucketName']
        key = body['s3']['key']
        languageCode = body['transcribe']['languageCode']
        maxSpeakers = body['transcribe']['maxSpeakers']

        if not key.endswith(".mp3") or not key.startswith("audios/"):
            logger.warning(f"Ignorando archivo no válido: {key}")
            return _resp(400, {"error": "Clave S3 inválida"})

        job_name = f"transcription-job-{uuid.uuid4()}"
        media_uri = f"s3://{bucketName}/{key}"
        output_key = f"transcripciones/{job_name}.json"

        transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat='mp3',
            LanguageCode=languageCode,
            OutputBucketName=output_bucket,
            OutputKey=output_key,
            Settings={
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': maxSpeakers
            }
        )
        logger.info(f"Transcripción iniciada para: {media_uri}")

        return _resp(200, {
            "message": "Transcripción iniciada correctamente.",
            "jobName": job_name,
            "outputLocation": f"s3://{output_bucket}/{output_key}"
        })
    except Exception as e:
        logger.error(f"Error al iniciar transcripción: {str(e)}")
        return _resp(500, {"error": str(e)})
