import json
import boto3
import uuid
import os
from datetime import datetime

# Inicializar clientes AWS
transcribe_client = boto3.client('transcribe')
dynamodb = boto3.resource('dynamodb')
apigateway_client = None  # Se inicializará si hay endpoint WebSocket

def lambda_handler(event, context):
    try:
        # Parsear el cuerpo del evento si viene de API Gateway
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event
        
        # Verificar que el evento contiene los datos correctamente
        if not body.get('s3') or not body.get('transcribe'):
            return {
                'statusCode': 400,
                'headers': get_cors_headers(),
                'body': json.dumps({
                    'error': 'Faltan datos requeridos: s3 o transcribe'
                })
            }
        
        s3_data = body['s3']
        transcribe_data = body['transcribe']
        
        # Validar datos de S3
        bucket_name = s3_data.get('bucketName')
        file_key = s3_data.get('key')
        
        if not bucket_name or not file_key:
            return {
                'statusCode': 400,
                'headers': get_cors_headers(),
                'body': json.dumps({
                    'error': 'Faltan bucketName o key en datos de S3'
                })
            }
        
        # Validar datos de transcripción
        language_code = transcribe_data.get('languageCode', 'es-ES')
        max_speakers = transcribe_data.get('maxSpeakers', 2)
        
        # Generar nombre único para el job
        job_name = f"transcription-job-{str(uuid.uuid4())}"
        
        # Obtener sessionId del evento (enviado por el frontend)
        session_id = body.get('sessionId', job_name)
        
        # Configurar la URI del archivo en S3
        media_uri = f"s3://{bucket_name}/{file_key}"
        
        # Configurar parámetros para el job de transcripción
        job_params = {
            'TranscriptionJobName': job_name,
            'Media': {
                'MediaFileUri': media_uri
            },
            'MediaFormat': get_media_format(file_key),
            'LanguageCode': language_code,
            'Settings': {
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': max_speakers,
                'ChannelIdentification': False
            },
            'OutputBucketName': bucket_name,
            'OutputKey': f'transcriptions/{job_name}.json'
        }
        
        print(f"Iniciando job de transcripción: {job_name}")
        print(f"Parámetros: {json.dumps(job_params, default=str)}")
        
        # Iniciar el job de transcripción
        response = transcribe_client.start_transcription_job(**job_params)
        
        print(f"Job iniciado exitosamente: {response.get('TranscriptionJob', {}).get('TranscriptionJobStatus', 'UNKNOWN')}")
        
        # Guardar la información del job en DynamoDB si la tabla existe
        save_job_info(job_name, session_id, bucket_name, file_key, language_code, max_speakers)
        
        # Notificar vía WebSocket que la transcripción ha comenzado
        notify_websocket_clients(session_id, {
            'type': 'TRANSCRIPTION_STARTED',
            'jobName': job_name,
            'status': 'IN_PROGRESS',
            'message': 'Transcripción iniciada correctamente',
            'timestamp': datetime.now().isoformat(),
            'stage': 'TRANSCRIBING',
            'details': {
                'languageCode': language_code,
                'maxSpeakers': max_speakers,
                'fileKey': file_key
            }
        })
        
        return {
            'statusCode': 200,
            'headers': get_cors_headers(),
            'body': json.dumps({
                'jobName': job_name,
                'sessionId': session_id,
                'status': 'IN_PROGRESS',
                'message': 'Job de transcripción iniciado correctamente',
                'details': {
                    'mediaUri': media_uri,
                    'languageCode': language_code,
                    'maxSpeakers': max_speakers,
                    'outputKey': f'transcriptions/{job_name}.json'
                }
            })
        }
        
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        
        # Notificar error vía WebSocket si es posible
        session_id = body.get('sessionId') if 'body' in locals() else None
        if session_id:
            notify_websocket_clients(session_id, {
                'type': 'ERROR',
                'message': f'Error al iniciar transcripción: {str(e)}',
                'timestamp': datetime.utcnow().isoformat(),
                'stage': 'TRANSCRIPTION_START',
                'error': str(e)
            })
        
        return {
            'statusCode': 500,
            'headers': get_cors_headers(),
            'body': json.dumps({
                'error': f'Error interno del servidor: {str(e)}'
            })
        }

def get_media_format(file_key):
    """Determina el formato del archivo basado en la extensión"""
    extension = file_key.lower().split('.')[-1]
    
    format_mapping = {
        'mp3': 'mp3',
        'mp4': 'mp4',
        'wav': 'wav',
        'flac': 'flac',
        'm4a': 'mp4',
        'ogg': 'ogg',
        'webm': 'webm',
        'amr': 'amr'
    }
    
    detected_format = format_mapping.get(extension, 'mp3')
    print(f"Formato detectado para {file_key}: {detected_format}")
    
    return detected_format

def get_cors_headers():
    """Retorna headers CORS estándar"""
    return {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'POST, OPTIONS, GET'
    }

def save_job_info(job_name, session_id, bucket_name, file_key, language_code, max_speakers):
    """Guarda información del job en DynamoDB si la tabla existe"""
    try:
        jobs_table_name = os.environ.get('JOBS_TABLE', 'transcription-jobs')
        jobs_table = dynamodb.Table(jobs_table_name)
        
        item = {
            'jobName': job_name,
            'sessionId': session_id,
            'status': 'IN_PROGRESS',
            'stage': 'TRANSCRIBING',
            'bucketName': bucket_name,
            'fileKey': file_key,
            'languageCode': language_code,
            'maxSpeakers': max_speakers,
            'createdAt': datetime.utcnow().isoformat(),
            'updatedAt': datetime.utcnow().isoformat(),
            'mediaUri': f"s3://{bucket_name}/{file_key}",
            'outputKey': f'transcriptions/{job_name}.json'
        }
        
        jobs_table.put_item(Item=item)
        print(f"Job info guardada en DynamoDB: {job_name}")
        
        return True
        
    except Exception as e:
        print(f"Error guardando en DynamoDB: {str(e)}")
        # No fallar si no se puede guardar en DynamoDB
        return False

def notify_websocket_clients(session_id, message):
    """Notifica a los clientes WebSocket conectados"""
    try:
        websocket_endpoint = os.environ.get('WEBSOCKET_API_ENDPOINT')
        if not websocket_endpoint:
            print("No WebSocket endpoint configurado, saltando notificación")
            return False
        
        # Inicializar cliente API Gateway Management si no existe
        global apigateway_client
        if not apigateway_client:
            apigateway_client = boto3.client(
                'apigatewaymanagementapi',
                endpoint_url=websocket_endpoint
            )
        
        # Buscar conexiones activas para esta sesión
        connections_table_name = os.environ.get('CONNECTIONS_TABLE', 'websocket-connections')
        connections_table = dynamodb.Table(connections_table_name)
        
        print(f"Buscando conexiones para sesión: {session_id}")
        
        # Buscar por sessionId
        response = connections_table.scan(
            FilterExpression='sessionId = :sid AND #status = :status',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':sid': session_id,
                ':status': 'CONNECTED'
            }
        )
        
        connections = response.get('Items', [])
        success_count = 0
        
        if not connections:
            print(f"No se encontraron conexiones activas para la sesión {session_id}")
            return False
        
        # Convertir mensaje a JSON
        message_data = json.dumps(message) if not isinstance(message, str) else message
        
        # Enviar mensaje a todas las conexiones de esta sesión
        for item in connections:
            connection_id = item['connectionId']
            try:
                apigateway_client.post_to_connection(
                    ConnectionId=connection_id,
                    Data=message_data
                )
                success_count += 1
                print(f"Mensaje enviado a conexión {connection_id}")
                
            except apigateway_client.exceptions.GoneException:
                print(f"Conexión {connection_id} ya no existe, eliminando")
                # Limpiar conexión inválida
                try:
                    connections_table.delete_item(Key={'connectionId': connection_id})
                except Exception as delete_error:
                    print(f"Error eliminando conexión: {delete_error}")
                    
            except Exception as send_error:
                print(f"Error enviando mensaje a {connection_id}: {send_error}")
        
        print(f"Mensaje enviado a {success_count}/{len(connections)} conexiones")
        return success_count > 0
                    
    except Exception as e:
        print(f"Error en notify_websocket_clients: {str(e)}")
        # No fallar si no se puede enviar notificación WebSocket
        return False

def validate_transcription_job_params(job_params):
    """Valida los parámetros del job de transcripción antes de enviarlo"""
    try:
        required_fields = ['TranscriptionJobName', 'Media', 'MediaFormat', 'LanguageCode']
        
        for field in required_fields:
            if field not in job_params:
                raise ValueError(f"Campo requerido faltante: {field}")
        
        # Validar Media URI
        media_uri = job_params['Media'].get('MediaFileUri', '')
        if not media_uri.startswith('s3://'):
            raise ValueError("Media URI debe ser una URL S3 válida")
        
        # Validar formato de idioma
        language_code = job_params['LanguageCode']
        if len(language_code.split('-')) != 2:
            raise ValueError("Código de idioma debe tener formato 'xx-XX' (ej: es-ES, en-US)")
        
        # Validar MaxSpeakerLabels si está presente
        if 'Settings' in job_params and 'MaxSpeakerLabels' in job_params['Settings']:
            max_speakers = job_params['Settings']['MaxSpeakerLabels']
            if not isinstance(max_speakers, int) or max_speakers < 2 or max_speakers > 10:
                raise ValueError("MaxSpeakerLabels debe ser un entero entre 2 y 10")
        
        print("Parámetros de transcripción validados correctamente")
        return True
        
    except Exception as e:
        print(f"Error validando parámetros: {str(e)}")
        raise e

def get_job_status(job_name):
    """Obtiene el estado actual de un job de transcripción"""
    try:
        response = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name
        )
        
        job = response.get('TranscriptionJob', {})
        status = job.get('TranscriptionJobStatus', 'UNKNOWN')
        
        print(f"Estado del job {job_name}: {status}")
        return status
        
    except Exception as e:
        print(f"Error obteniendo estado del job {job_name}: {str(e)}")
        return 'ERROR'

def cleanup_failed_job(job_name):
    """Limpia recursos de un job que falló"""
    try:
        print(f"Limpiando recursos del job fallido: {job_name}")
        
        # Actualizar estado en DynamoDB
        jobs_table_name = os.environ.get('JOBS_TABLE', 'transcription-jobs')
        jobs_table = dynamodb.Table(jobs_table_name)
        
        jobs_table.update_item(
            Key={'jobName': job_name},
            UpdateExpression='SET #status = :status, updatedAt = :timestamp, errorMessage = :error',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': 'FAILED',
                ':timestamp': datetime.utcnow().isoformat(),
                ':error': 'Job failed during initialization'
            }
        )
        
        print(f"Estado del job {job_name} actualizado a FAILED")
        
    except Exception as e:
        print(f"Error en limpieza del job {job_name}: {str(e)}")