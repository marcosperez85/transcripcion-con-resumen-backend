from datetime import datetime
import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
output_bucket = os.environ['BUCKET']

dynamodb = boto3.resource("dynamodb")
usage_table = dynamodb.Table(os.environ["USAGE_TABLE"])

def lambda_handler(event, context):
    # Verifica que el evento contiene los datos correctamente
    logger.info(f"Received event: {json.dumps(event)}")

    # Tomar SIEMPRE el record S3
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = record['s3']['object']['key']

    if not key.endswith(".json") or not key.startswith("transcripciones/"):
        logger.warning(f"Ignorando archivo no válido: {key}")
        return

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        transcript_data = json.loads(response['Body'].read().decode('utf-8'))

        items = transcript_data['results']['items']
        # calcular duración del audio
        duration_seconds = 0

        if items:
            for item in reversed(items):
                if 'end_time' in item:
                    duration_seconds = float(item['end_time'])
                    break

        for item in reversed(items):
            if 'end_time' in item:
                duration_seconds = float(item['end_time'])
                break
        speaker_segments = transcript_data['results'].get('speaker_labels', {}).get('segments', [])

        speaker_map = {}
        for segment in speaker_segments:
            speaker = segment['speaker_label']
            for item in segment['items']:
                speaker_map[item['start_time']] = speaker

        output_text = ""
        current_speaker = None

        for item in items:
            if item['type'] == 'punctuation':
                output_text += item['alternatives'][0]['content']
            else:
                start_time = item.get('start_time')
                speaker = speaker_map.get(start_time)

                if speaker != current_speaker:
                    current_speaker = speaker
                    output_text += f"\n\n{speaker}: "

                output_text += item['alternatives'][0]['content'] + " "

        # Guardar archivo .txt
        filename = os.path.basename(key).replace(".json", ".txt")
        job_name = filename.replace(".txt", "")
        user_id = job_name.split("-")[0]

        txt_key = f"transcripciones-formateadas/{filename}"
        s3_client.put_object(
            Bucket=bucket,
            Key=txt_key,
            Body=output_text.strip().encode('utf-8'),
            ContentType='text/plain'
        )

        logger.info(f"Archivo TXT guardado en: s3://{bucket}/{txt_key}")

        usage_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="""
                SET totalSeconds = if_not_exists(totalSeconds, :zero) + :delta,
                    limitSeconds = if_not_exists(limitSeconds, :limit),
                    updatedAt = :now
            """,
            ConditionExpression="attribute_not_exists(totalSeconds) OR totalSeconds < :limit",
            ExpressionAttributeValues={
                ":delta": duration_seconds,
                ":zero": 0,
                ":limit": 1800,
                ":now": datetime.utcnow().isoformat()
            }
        )

        logger.info(f"Duración {duration_seconds}s agregada al usuario {user_id}")

    except Exception as e:
        logger.error(f"Error al procesar transcripción: {str(e)}")
        raise
