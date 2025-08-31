import json
import boto3
import os
from datetime import datetime

# Inicializar recursos AWS
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
bedrock_runtime = boto3.client('bedrock-runtime')

# Nombres de tablas DynamoDB
connections_table_name = os.environ.get('CONNECTIONS_TABLE', 'websocket-connections')
jobs_table_name = os.environ.get('JOBS_TABLE', 'transcription-jobs')

connections_table = dynamodb.Table(connections_table_name)
jobs_table = dynamodb.Table(jobs_table_name)

# Cliente API Gateway Management
apigateway_client = None

def lambda_handler(event, context):

    # Maneja eventos de AWS Transcribe y otros servicios para notificar vía WebSocket
    try:
        print(f"Evento recibido: {json.dumps(event, default=str)}")
        
        # Determinar el tipo de evento
        source = event.get('source', '')
        detail_type = event.get('detail-type', '')
        
        if source == 'aws.transcribe' and detail_type == 'Transcribe Job State Change':
            return handle_transcribe_event(event)
        elif source == 'aws.s3':
            return handle_s3_event(event)
        else:
            print(f"Evento no reconocido - Source: {source}, DetailType: {detail_type}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Evento no procesado'})
            }
            
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Error procesando evento: {str(e)}'})
        }

def handle_transcribe_event(event):
    
    # Maneja eventos de cambio de estado de AWS Transcribe
    try:
        detail = event.get('detail', {})
        job_name = detail.get('TranscriptionJobName', '')
        job_status = detail.get('TranscriptionJobStatus', '')
        
        print(f"Evento Transcribe - Job: {job_name}, Status: {job_status}")
        
        if not job_name:
            print("Error: No se encontró nombre del job en el evento")
            return {'statusCode': 400, 'body': 'Job name not found'}
        
        # Buscar información del job en DynamoDB
        job_info = get_job_info(job_name)
        if not job_info:
            print(f"Warning: No se encontró información del job {job_name} en DynamoDB")
            return {'statusCode': 404, 'body': 'Job info not found'}
        
        session_id = job_info.get('sessionId', job_name)
        
        # Procesar según el estado del job
        if job_status == 'COMPLETED':
            return handle_transcribe_completed(job_name, session_id, detail, job_info)
        elif job_status == 'FAILED':
            return handle_transcribe_failed(job_name, session_id, detail, job_info)
        elif job_status == 'IN_PROGRESS':
            return handle_transcribe_in_progress(job_name, session_id, detail, job_info)
        else:
            print(f"Estado de job no manejado: {job_status}")
            return {'statusCode': 200, 'body': 'Status not handled'}
            
    except Exception as e:
        print(f"Error en handle_transcribe_event: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_transcribe_completed(job_name, session_id, detail, job_info):
    
    # Maneja la finalización exitosa de un job de transcripción
    try:
        print(f"Transcripción completada para job: {job_name}")
        
        # Actualizar estado del job en DynamoDB
        update_job_status(job_name, 'COMPLETED', 'TRANSCRIPTION_DONE')
        
        # Obtener la transcripción desde S3
        transcript_uri = detail.get('Transcript', {}).get('TranscriptFileUri', '')
        
        # Notificar vía WebSocket que la transcripción está completa
        notification = {
            'type': 'TRANSCRIPTION_COMPLETED',
            'jobName': job_name,
            'status': 'COMPLETED',
            'message': 'Transcripción completada exitosamente. Iniciando formateo...',
            'timestamp': datetime.utcnow().isoformat(),
            'transcriptUri': transcript_uri
        }
        
        broadcast_to_session(session_id, notification)
        
        # El formateo se iniciará automáticamente por la notificación S3
        # cuando Transcribe guarde el archivo .json en el bucket
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Transcription completed notification sent'})
        }
        
    except Exception as e:
        print(f"Error en handle_transcribe_completed: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_transcribe_failed(job_name, session_id, detail, job_info):
    
    # Maneja la falla de un job de transcripción
    try:
        print(f"Transcripción falló para job: {job_name}")
        
        failure_reason = detail.get('FailureReason', 'Razón desconocida')
        
        # Actualizar estado del job en DynamoDB
        update_job_status(job_name, 'FAILED', 'TRANSCRIPTION_FAILED', failure_reason)
        
        # Notificar error vía WebSocket
        error_notification = {
            'type': 'ERROR',
            'jobName': job_name,
            'status': 'FAILED',
            'message': f'Error en la transcripción: {failure_reason}',
            'timestamp': datetime.utcnow().isoformat(),
            'stage': 'TRANSCRIPTION',
            'error': failure_reason
        }
        
        broadcast_to_session(session_id, error_notification)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Transcription failure notification sent'})
        }
        
    except Exception as e:
        print(f"Error en handle_transcribe_failed: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_transcribe_in_progress(job_name, session_id, detail, job_info):
    
    # Maneja el estado en progreso de un job de transcripción
    try:
        print(f"Transcripción en progreso para job: {job_name}")
        
        # Actualizar timestamp en DynamoDB
        update_job_status(job_name, 'IN_PROGRESS', 'TRANSCRIBING')
        
        # Notificar progreso vía WebSocket
        progress_notification = {
            'type': 'TRANSCRIPTION_PROGRESS',
            'jobName': job_name,
            'status': 'IN_PROGRESS',
            'message': 'Transcripción en progreso...',
            'timestamp': datetime.utcnow().isoformat(),
            'stage': 'TRANSCRIBING'
        }
        
        broadcast_to_session(session_id, progress_notification)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Transcription progress notification sent'})
        }
        
    except Exception as e:
        print(f"Error en handle_transcribe_in_progress: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_s3_event(event):
    
    # Maneja eventos S3 para detectar cuando se completan etapas del proceso
    try:
        print("Procesando evento S3")
        
        records = event.get('Records', [])
        
        for record in records:
            bucket_name = record.get('s3', {}).get('bucket', {}).get('name', '')
            object_key = record.get('s3', {}).get('object', {}).get('key', '')
            event_name = record.get('eventName', '')
            
            print(f"Evento S3: {event_name} - Bucket: {bucket_name}, Key: {object_key}")
            
            # Detectar cuando se completa el formateo
            if object_key.startswith('transcripciones-formateadas/') and object_key.endswith('.txt'):
                handle_formatting_completed(bucket_name, object_key)
            
            # Detectar cuando se completa el resumen
            elif object_key.startswith('resumenes/') and object_key.endswith('.txt'):
                handle_summary_completed(bucket_name, object_key)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'S3 events processed'})
        }
        
    except Exception as e:
        print(f"Error en handle_s3_event: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_formatting_completed(bucket_name, object_key):
    
    # Maneja la finalización del formateo de transcripción
    try:
        print(f"Formateo completado para: {object_key}")
        
        # Extraer job name del nombre del archivo
        job_name = extract_job_name_from_key(object_key)
        if not job_name:
            print("No se pudo extraer job name del object key")
            return
        
        job_info = get_job_info(job_name)
        if not job_info:
            print(f"No se encontró información del job: {job_name}")
            return
        
        session_id = job_info.get('sessionId', job_name)
        
        # Actualizar estado del job
        update_job_status(job_name, 'IN_PROGRESS', 'FORMATTING_COMPLETED')
        
        # Notificar que el formateo está completo y se inicia el resumen
        notification = {
            'type': 'FORMATTING_COMPLETED',
            'jobName': job_name,
            'status': 'IN_PROGRESS',
            'message': 'Formateo completado. Iniciando resumen...',
            'timestamp': datetime.utcnow().isoformat(),
            'stage': 'SUMMARIZING',
            'formattedTextKey': object_key
        }
        
        broadcast_to_session(session_id, notification)
        
    except Exception as e:
        print(f"Error en handle_formatting_completed: {str(e)}")

def handle_summary_completed(bucket_name, object_key):
    
    # Maneja la finalización del resumen
    try:
        print(f"Resumen completado para: {object_key}")
        
        # Extraer job name del nombre del archivo
        job_name = extract_job_name_from_key(object_key)
        if not job_name:
            print("No se pudo extraer job name del object key")
            return
        
        job_info = get_job_info(job_name)
        if not job_info:
            print(f"No se encontró información del job: {job_name}")
            return
        
        session_id = job_info.get('sessionId', job_name)
        
        # Leer la transcripción formateada y el resumen desde S3
        formatted_key = object_key.replace('resumenes/', 'transcripciones-formateadas/')
        
        try:
            # Leer transcripción formateada
            transcription_response = s3_client.get_object(Bucket=bucket_name, Key=formatted_key)
            transcription_text = transcription_response['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Error leyendo transcripción: {e}")
            transcription_text = "Error al obtener transcripción"
        
        try:
            # Leer resumen
            summary_response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            summary_text = summary_response['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Error leyendo resumen: {e}")
            summary_text = "Error al obtener resumen"
        
        # Actualizar estado del job
        update_job_status(job_name, 'COMPLETED', 'ALL_COMPLETED')
        
        # Notificar que todo el proceso está completo con los resultados
        final_notification = {
            'type': 'PROCESS_COMPLETED',
            'jobName': job_name,
            'status': 'COMPLETED',
            'message': 'Proceso completado exitosamente',
            'timestamp': datetime.utcnow().isoformat(),
            'stage': 'COMPLETED',
            'results': {
                'transcription': transcription_text,
                'summary': summary_text,
                'transcriptionKey': formatted_key,
                'summaryKey': object_key
            }
        }
        
        broadcast_to_session(session_id, final_notification)
        
    except Exception as e:
        print(f"Error en handle_summary_completed: {str(e)}")

def get_job_info(job_name):
    
    # Obtiene información del job desde DynamoDB
    try:
        response = jobs_table.get_item(Key={'jobName': job_name})
        return response.get('Item')
    except Exception as e:
        print(f"Error obteniendo info del job {job_name}: {str(e)}")
        return None

def update_job_status(job_name, status, stage, error_message=None):
    
    # Actualiza el estado del job en DynamoDB
    try:
        update_expression = 'SET #status = :status, #stage = :stage, updatedAt = :timestamp'
        expression_values = {
            ':status': status,
            ':stage': stage,
            ':timestamp': datetime.utcnow().isoformat()
        }
        expression_names = {
            '#status': 'status',
            '#stage': 'stage'
        }
        
        if error_message:
            update_expression += ', errorMessage = :error'
            expression_values[':error'] = error_message
        
        jobs_table.update_item(
            Key={'jobName': job_name},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values
        )
        
        print(f"Estado del job {job_name} actualizado: {status} - {stage}")
        
    except Exception as e:
        print(f"Error actualizando estado del job {job_name}: {str(e)}")

def extract_job_name_from_key(object_key):
    
    # Extrae el job name del object key de S3
    try:
        # Los archivos siguen el patrón: prefijo/job-name.extension
        filename = object_key.split('/')[-1]  # Obtener solo el nombre del archivo
        job_name = filename.rsplit('.', 1)[0]  # Remover extensión
        return job_name
    except Exception as e:
        print(f"Error extrayendo job name de {object_key}: {str(e)}")
        return None

def broadcast_to_session(session_id, message):
    
    # Envía un mensaje a todas las conexiones WebSocket de una sesión
    global apigateway_client
    
    try:
        # Inicializar cliente si no existe
        if not apigateway_client:
            websocket_endpoint = os.environ.get('WEBSOCKET_API_ENDPOINT')
            if not websocket_endpoint:
                print("Error: WEBSOCKET_API_ENDPOINT no configurado")
                return 0
                
            apigateway_client = boto3.client(
                'apigatewaymanagementapi',
                endpoint_url=websocket_endpoint
            )
        
        print(f"Broadcasting mensaje a sesión: {session_id}")
        
        # Buscar todas las conexiones activas para esta sesión
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
            return 0
        
        # Convertir mensaje a JSON
        message_data = json.dumps(message) if not isinstance(message, str) else message
        
        # Enviar mensaje a cada conexión
        for connection in connections:
            connection_id = connection['connectionId']
            try:
                apigateway_client.post_to_connection(
                    ConnectionId=connection_id,
                    Data=message_data
                )
                success_count += 1
                print(f"Mensaje enviado a conexión {connection_id}")
                
            except apigateway_client.exceptions.GoneException:
                print(f"Conexión {connection_id} ya no existe, eliminando")
                try:
                    connections_table.delete_item(Key={'connectionId': connection_id})
                except:
                    pass
                    
            except Exception as e:
                print(f"Error enviando mensaje a {connection_id}: {str(e)}")
        
        print(f"Mensaje enviado a {success_count}/{len(connections)} conexiones de la sesión {session_id}")
        return success_count
        
    except Exception as e:
        print(f"Error en broadcast_to_session: {str(e)}")
        return 0