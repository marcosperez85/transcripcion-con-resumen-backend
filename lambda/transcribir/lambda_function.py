import json
import boto3
import uuid
import os
import urllib.parse
from datetime import datetime

# Inicializar clientes AWS
transcribe_client = boto3.client('transcribe')
s3_client = boto3.client('s3')

# Variables de entorno
TRANSCRIPTION_BUCKET = os.environ.get('TRANSCRIPTION_BUCKET')
WEBSOCKET_API_ENDPOINT = os.environ.get('WEBSOCKET_API_ENDPOINT')

def lambda_handler(event, context):
    """
    Función Lambda que se ejecuta cuando se sube un archivo a S3
    e inicia la transcripción con AWS Transcribe
    """
    try:
        print(f"Evento recibido: {json.dumps(event, default=str)}")
        
        # Procesar cada record del evento S3
        for record in event['Records']:
            # Verificar que es un evento de S3
            if record['eventSource'] != 'aws:s3':
                print(f"Evento ignorado - no es de S3: {record['eventSource']}")
                continue
                
            # Obtener información del archivo subido
            bucket_name = record['s3']['bucket']['name']
            object_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
            
            print(f"Procesando archivo: s3://{bucket_name}/{object_key}")
            
            # Verificar que el archivo tiene una extensión de audio/video válida
            if not is_valid_media_file(object_key):
                print(f"Archivo ignorado - formato no válido: {object_key}")
                continue
            
            # Extraer session_id del path del objeto (asumiendo estructura: sessions/{session_id}/audio.mp3)
            session_id = extract_session_id_from_path(object_key)
            if not session_id:
                print(f"No se pudo extraer session_id del path: {object_key}")
                continue
            
            # Iniciar proceso de transcripción
            result = start_transcription(bucket_name, object_key, session_id)
            
            # Notificar via WebSocket del inicio de transcripción
            if result['success']:
                notify_websocket_transcription_started(session_id, result['job_name'])
            else:
                notify_websocket_error(session_id, result['error'])
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Procesamiento completado',
                'processedRecords': len(event['Records'])
            })
        }
        
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error procesando evento S3: {str(e)}'})
        }

def is_valid_media_file(file_path):
    """Verifica si el archivo tiene una extensión válida para transcripción"""
    valid_extensions = ['.mp3', '.mp4', '.wav', '.flac', '.ogg', '.amr', '.webm', '.m4a']
    return any(file_path.lower().endswith(ext) for ext in valid_extensions)

def extract_session_id_from_path(object_key):
    """
    Extrae el session_id del path del objeto S3
    Asume estructura: sessions/{session_id}/filename.ext
    """
    try:
        parts = object_key.split('/')
        if len(parts) >= 2 and parts[0] == 'sessions':
            return parts[1]
        return None
    except Exception as e:
        print(f"Error extrayendo session_id: {str(e)}")
        return None

def start_transcription(bucket_name, object_key, session_id):
    """Inicia el trabajo de transcripción en AWS Transcribe"""
    try:
        # Generar nombre único para el trabajo
        job_name = f"transcribe-job-{session_id}-{uuid.uuid4().hex[:8]}"
        
        # URI del archivo en S3
        media_uri = f"s3://{bucket_name}/{object_key}"
        
        # URI de salida (mismo bucket, carpeta de resultados)
        output_key = f"sessions/{session_id}/transcriptions/"
        output_bucket_name = TRANSCRIPTION_BUCKET or bucket_name
        
        print(f"Iniciando transcripción - Job: {job_name}, URI: {media_uri}")
        
        # Configuración del trabajo de transcripción
        transcribe_config = {
            'TranscriptionJobName': job_name,
            'LanguageCode': 'es-ES',  # Español por defecto
            'Media': {
                'MediaFileUri': media_uri
            },
            'OutputBucketName': output_bucket_name,
            'OutputKey': output_key,
            'Settings': {
                'ShowSpeakerLabels': True,
                'MaxSpeakerLabels': 10,
                'ShowAlternatives': True,
                'MaxAlternatives': 3
            }
        }
        
        # Detectar formato de archivo y configurar si es necesario
        file_format = detect_media_format(object_key)
        if file_format:
            transcribe_config['MediaFormat'] = file_format
        
        # Iniciar trabajo de transcripción
        response = transcribe_client.start_transcription_job(**transcribe_config)
        
        print(f"Trabajo de transcripción iniciado exitosamente: {job_name}")
        
        return {
            'success': True,
            'job_name': job_name,
            'transcription_job': response['TranscriptionJob']
        }
        
    except Exception as e:
        error_msg = f"Error iniciando transcripción: {str(e)}"
        print(error_msg)
        return {
            'success': False,
            'error': error_msg
        }

def detect_media_format(file_path):
    """Detecta el formato del archivo de media basado en la extensión"""
    extension = file_path.lower().split('.')[-1]
    
    format_mapping = {
        'mp3': 'mp3',
        'mp4': 'mp4',
        'wav': 'wav',
        'flac': 'flac',
        'ogg': 'ogg',
        'amr': 'amr',
        'webm': 'webm',
        'm4a': 'm4a'
    }
    
    return format_mapping.get(extension)

def notify_websocket_transcription_started(session_id, job_name):
    """Notifica via WebSocket que se inició la transcripción"""
    try:
        if not WEBSOCKET_API_ENDPOINT:
            print("WEBSOCKET_API_ENDPOINT no configurado")
            return
        
        message = {
            'type': 'TRANSCRIPTION_STARTED',
            'sessionId': session_id,
            'jobName': job_name,
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'IN_PROGRESS',
            'message': 'Transcripción iniciada correctamente'
        }
        
        # Invocar la función de WebSocket para enviar el mensaje
        invoke_websocket_function(session_id, message)
        
    except Exception as e:
        print(f"Error notificando inicio de transcripción: {str(e)}")

def notify_websocket_error(session_id, error_message):
    """Notifica via WebSocket que ocurrió un error"""
    try:
        if not WEBSOCKET_API_ENDPOINT:
            print("WEBSOCKET_API_ENDPOINT no configurado")
            return
        
        message = {
            'type': 'TRANSCRIPTION_ERROR',
            'sessionId': session_id,
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'ERROR',
            'error': error_message
        }
        
        # Invocar la función de WebSocket para enviar el mensaje
        invoke_websocket_function(session_id, message)
        
    except Exception as e:
        print(f"Error notificando error de transcripción: {str(e)}")

def invoke_websocket_function(session_id, message):
    """Invoca la función Lambda de WebSocket para enviar mensajes"""
    try:
        lambda_client = boto3.client('lambda')
        
        # Nombre de la función de WebSocket (debería ser variable de entorno)
        websocket_function_name = os.environ.get('WEBSOCKET_FUNCTION_NAME')
        
        if not websocket_function_name:
            print("WEBSOCKET_FUNCTION_NAME no configurado")
            return
        
        payload = {
            'action': 'broadcast',
            'sessionId': session_id,
            'message': message
        }
        
        response = lambda_client.invoke(
            FunctionName=websocket_function_name,
            InvocationType='Event',  # Asíncrono
            Payload=json.dumps(payload)
        )
        
        print(f"Mensaje WebSocket enviado para sesión {session_id}")
        
    except Exception as e:
        print(f"Error invocando función WebSocket: {str(e)}")