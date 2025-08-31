import json
import boto3
import os
from datetime import datetime

# Clientes AWS
dynamodb = boto3.resource('dynamodb')
apigateway_client = boto3.client('apigatewaymanagementapi')

# Tablas
connections_table = dynamodb.Table(os.environ['CONNECTIONS_TABLE'])

def lambda_handler(event, context):
    
    # Maneja conexiones y mensajes WebSocket
    try:
        route_key = event.get('requestContext', {}).get('routeKey')
        # connection_id = event.get('requestContext', {}).get('connectionId')
        
        if route_key == '$connect':
            return handle_connect(event, context)
        elif route_key == '$disconnect':
            return handle_disconnect(event, context)
        else:
            return handle_message(event, context)
            
    except Exception as e:
        print(f"Error en lambda_handler: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_connect(event, context):
    
    # Maneja nuevas conexiones WebSocket
    try:
        connection_id = event['requestContext']['connectionId']
        
        # Obtener sessionId de los query parameters
        query_params = event.get('queryStringParameters') or {}
        session_id = query_params.get('sessionId', f'session_{connection_id}')
        
        # Guardar conexión en DynamoDB
        connections_table.put_item(
            Item={
                'connectionId': connection_id,
                'sessionId': session_id,
                'status': 'CONNECTED',
                'connectedAt': datetime.now().isoformat(),
                'ttl': int((datetime.now()).timestamp()) + 86400  # TTL 24 horas
            }
        )
        
        print(f"Nueva conexión registrada: {connection_id} para sesión {session_id}")
        
        # Enviar mensaje de confirmación
        send_message_to_connection(connection_id, {
            'type': 'CONNECTION_ESTABLISHED',
            'sessionId': session_id,
            'connectionId': connection_id,
            'timestamp': datetime.now().isoformat()
        })
        
        return {'statusCode': 200, 'body': 'Conectado'}
        
    except Exception as e:
        print(f"Error en handle_connect: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_disconnect(event, context):
    
    # Maneja desconexiones WebSocket
    try:
        connection_id = event['requestContext']['connectionId']
        
        # Actualizar estado en DynamoDB
        connections_table.update_item(
            Key={'connectionId': connection_id},
            UpdateExpression='SET #status = :status, disconnectedAt = :timestamp',
            ExpressionAttributeNames={'#status': 'status'},
            ExpressionAttributeValues={
                ':status': 'DISCONNECTED',
                ':timestamp': datetime.now().isoformat()
            }
        )
        
        print(f"Conexión desconectada: {connection_id}")
        
        return {'statusCode': 200, 'body': 'Desconectado'}
        
    except Exception as e:
        print(f"Error en handle_disconnect: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def handle_message(event, context):
    
    # Maneja mensajes WebSocket
    try:
        connection_id = event['requestContext']['connectionId']
        body = json.loads(event.get('body', '{}'))
        message_type = body.get('type', 'UNKNOWN')
        
        print(f"Mensaje recibido de {connection_id}: {message_type}")
        
        if message_type == 'PING':
            # Responder a ping con pong
            send_message_to_connection(connection_id, {
                'type': 'PONG',
                'timestamp': datetime.now().isoformat()
            })
        
        return {'statusCode': 200, 'body': 'Mensaje procesado'}
        
    except Exception as e:
        print(f"Error en handle_message: {str(e)}")
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}

def send_message_to_connection(connection_id, message):
    
    # Envía un mensaje a una conexión específica
    try:
        # Configurar endpoint de API Gateway Management
        endpoint_url = os.environ['WEBSOCKET_API_ENDPOINT'].replace('wss://', 'https://').replace('ws://', 'http://')
        
        client = boto3.client('apigatewaymanagementapi', endpoint_url=endpoint_url)
        
        client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(message)
        )
        
        print(f"Mensaje enviado a conexión {connection_id}")
        return True
        
    except client.exceptions.GoneException:
        print(f"Conexión {connection_id} ya no existe")
        # Limpiar conexión obsoleta
        try:
            connections_table.delete_item(Key={'connectionId': connection_id})
        except Exception as e:
            # Catch any other unexpected errors
            print(f"Unexpected error deleting connection {connection_id}: {e}")
        return False
        
    except Exception as e:
        print(f"Error enviando mensaje a {connection_id}: {str(e)}")
        return False

def broadcast_to_session(session_id, message):
    
    # Envía un mensaje a todas las conexiones de una sesión específica
    try:
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
        
        # Enviar mensaje a cada conexión
        for connection in connections:
            connection_id = connection['connectionId']
            if send_message_to_connection(connection_id, message):
                success_count += 1
        
        print(f"Mensaje enviado a {success_count}/{len(connections)} conexiones de la sesión {session_id}")
        return success_count
        
    except Exception as e:
        print(f"Error en broadcast_to_session: {str(e)}")
        return 0