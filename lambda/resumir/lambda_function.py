import json
import boto3
import os
import logging
import os

# Configuración del logger para CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Nombre del bucket de entrada y salida
inputBucketName = "transcripcion-con-resumen"
# outputBucketName = "transcripcion-con-resumen"

output_bucket = os.environ['BUCKET']

# Crear clientes de AWS
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Extraer bucket y key del evento
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        logger.info(f"Archivo recibido: s3://{bucket}/{key}")

        # Leer archivo desde S3
        response = s3.get_object(Bucket=bucket, Key=key)
        text = response['Body'].read().decode('utf-8')

        # Preparar el prompt de resumen
        summary_prompt = f"Summarize the following text:\n{text}. Create a bullet list with the main topics. Keep source original language"

        # Preparar el cuerpo de la solicitud para Bedrock
        kwargs = {
            "modelId": "amazon.titan-text-express-v1",
            "contentType": "application/json",
            "accept": "*/*",
            "body": json.dumps({
                "inputText": summary_prompt,
                "textGenerationConfig": {
                    "maxTokenCount": 1024,
                    "temperature": 0.7,
                    "topP": 0.9,
                    "stopSequences": []
                }
            })
        }

        # Llamar al modelo de Bedrock
        bedrock_response = bedrock.invoke_model(**kwargs)
        response_body = json.loads(bedrock_response['body'].read())
        summary = response_body['results'][0]['outputText']

        # Extraer solo el nombre del archivo original
        filename = os.path.basename(key)

        # Cambiar nombre del archivo para el resumen
        summary_filename = filename.replace('.txt', '_summary.txt')

        # Guardar el resumen bajo la carpeta 'resumenes/' sin subcarpetas intermedias
        output_key = f"resumenes/{summary_filename}"

        # Subir el resumen a S3
        s3.put_object(
            Bucket=outputBucketName,
            Key=output_key,
            Body=summary
        )

        logger.info(f"Resumen guardado en: s3://{outputBucketName}/{output_key}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Summary generated: {output_key}')
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error processing file: {str(e)}")
        }
