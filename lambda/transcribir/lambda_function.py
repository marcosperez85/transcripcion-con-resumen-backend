import boto3
import uuid
import logging
import json
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

transcribe_client = boto3.client('transcribe')
output_bucket = os.environ['BUCKET']

def lambda_handler(event, context):
    # Verifica que el evento contiene los datos correctamente
    logger.info(f"Received event: {json.dumps(event)}")

    if 'body' in event:
        try:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        except Exception as e:
            logger.error(f"Error al parsear 'body': {str(e)}")
            raise
    else:
        body = event



    logger.info(f"El body del mensaje contiene: {body}")
    
    bucketName = body['s3']['bucketName']    
    key = body['s3']['key']

    languageCode = body['transcribe']['languageCode']
    maxSpeakers = body['transcribe']['maxSpeakers']

    if not key.endswith(".mp3") or not key.startswith("audios/"):
        logger.warning(f"Ignorando archivo no válido: {key}")
        return

    job_name = f"transcription-job-{uuid.uuid4()}"
    media_uri = f"s3://{bucketName}/{key}"
    output_key = f"transcripciones/{job_name}.json"

    try:
        transcribe_client.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': media_uri},
            MediaFormat='mp3',
            LanguageCode= languageCode,
            OutputBucketName=output_bucket,
            OutputKey=output_key,
            Settings={
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': maxSpeakers
            }
        )
        logger.info(f"Transcripción iniciada para: {media_uri}")
    
    except Exception as e:
        logger.error(f"Error al iniciar transcripción: {str(e)}")
        raise
    
    return {
        'statusCode': 200,
            "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        'body': json.dumps({
            'message': 'Transcripción iniciada correctamente.',
            'jobName': job_name,
            'outputLocation': f"s3://{output_bucket}/transcripciones/{job_name}.json"
        })
    }

    
