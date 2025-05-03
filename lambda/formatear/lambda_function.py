import boto3
import json
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

# output_bucket = "transcripcion-con-resumen"
output_bucket = os.environ['BUCKET']

def lambda_handler(event, context):
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
        txt_key = key.replace(".json", ".txt")
        s3_client.put_object(
            Bucket=bucket,
            Key=txt_key,
            Body=output_text.strip().encode('utf-8'),
            ContentType='text/plain'
        )

        logger.info(f"Archivo TXT guardado en: s3://{bucket}/{txt_key}")

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "transcripcion": output_text.strip()
            })
        }

    except Exception as e:
        logger.error(f"Error al procesar transcripción: {str(e)}")
        raise
