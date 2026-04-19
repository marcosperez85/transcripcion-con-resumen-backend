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
        # Extraer el user_id del key (transcripciones/{user_id}/{job_name}.json)
        user_id = key.split("/")[1]
        job_name = os.path.basename(key).replace(".json", "")

        response = s3_client.get_object(Bucket=bucket, Key=key)
        transcript_data = json.loads(response['Body'].read().decode('utf-8'))

        items = transcript_data['results']['items']
        
        # Calcular duración REAL del audio a partir de la transcripción
        duration_seconds = 0

        if items:
            for item in reversed(items):
                if 'end_time' in item:
                    duration_seconds = float(item['end_time'])
                    break

        logger.info(f"Real audio duration calculated: {duration_seconds} seconds")

        # Procesar la transcripción para formatearla
        speaker_segments = transcript_data['results'].get('speaker_labels', {}).get('segments', [])

        speaker_map = {}
        for segment in speaker_segments:
            speaker = segment['speaker_label']
            for item in segment['items']:
                speaker_map[item['start_time']] = speaker

        output_parts = []
        current_speaker = None

        for item in items:
            if item['type'] == 'punctuation':
                output_parts.append(item['alternatives'][0]['content'])
            else:
                start_time = item.get('start_time')
                speaker = speaker_map.get(start_time)

                if speaker != current_speaker:
                    current_speaker = speaker
                    output_parts.append(f"\n\n{speaker}: ")

                # 👇 SIEMPRE agregar texto
                output_parts.append(item['alternatives'][0]['content'] + " ")

        output_text = "".join(output_parts)

        # Guardar archivo .txt
        filename = os.path.basename(key).replace(".json", ".txt")
        txt_key = f"transcripciones-formateadas/{user_id}/{filename}"

        txt_key = f"transcripciones-formateadas/{user_id}/{filename}"
        s3_client.put_object(
            Bucket=bucket,
            Key=txt_key,
            Body=output_text.strip().encode('utf-8'),
            ContentType='text/plain'
        )

        logger.info(f"Archivo TXT guardado en: s3://{bucket}/{txt_key}")

        # Actualizar DynamoDB con la duración real y marcar como formateado
        # IMPORTANTE: No sumamos todavía a totalSeconds, solo actualizamos pendingSeconds
        # con la duración real, para que el proceso final (resumir) haga la suma
        usage_table.update_item(
            Key={"userId": user_id},
            UpdateExpression="""
            SET 
                totalSeconds = if_not_exists(totalSeconds, :zero) + :duration,
                pendingSeconds = if_not_exists(pendingSeconds, :zero) - :duration,
                lastProcessedAt = :now,
                jobStatus = :status
            """,
            ExpressionAttributeValues={
                ":zero": 0,
                ":duration": duration_seconds,
                ":now": datetime.utcnow().isoformat(),
                ":status": "FORMATTED"
            }
        )

        logger.info(f"USER {user_id} processed audio duration: {duration_seconds}s")
        logger.info(f"USER {user_id} ACTUAL_USAGE updated with duration: {duration_seconds}s")
        

    except Exception as e:
        logger.error(f"Error al procesar transcripción: {str(e)}")
        raise
