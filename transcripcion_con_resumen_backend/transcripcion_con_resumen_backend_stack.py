import aws_cdk as cdk
from aws_cdk import (
    Duration,
    Stack,
    CfnOutput,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_iam as iam,
    aws_apigateway as apigateway,
    aws_apigatewayv2 as apigwv2,
    aws_apigatewayv2_integrations as apigwv2_integrations,  # AGREGAR ESTA LÍNEA
    aws_s3_notifications as s3n,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as events_targets
)
from constructs import Construct


class TranscripcionConResumenBackendStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ========== S3 BUCKET (CREAR PRIMERO) ==========
        
        # Crear bucket único con sufijo único
        self.bucket = s3.Bucket(
            self, "transcripcion-con-resumen",
            bucket_name="transcripcion-con-resumen",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,
            public_read_access=False,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            cors=[
                s3.CorsRule(
                    allowed_methods=[s3.HttpMethods.GET, s3.HttpMethods.PUT, s3.HttpMethods.POST],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3600
                )
            ]
        )

        # Prefijos para organización
        self.PFX_AUDIOS = "audios/"
        self.PFX_TRANSCRIPCIONES = "transcripciones/"
        self.PFX_TRANSCRIPCIONES_FMT = "transcripciones-formateadas/"
        self.PFX_RESUMENES = "resumenes/"
        self.PFX_STATIC = "static/"

        # ========== TABLAS DYNAMODB ==========
        
        # Tabla DynamoDB para conexiones WebSocket
        connections_table = dynamodb.Table(
            self, "proyecto1-connections-table",
            table_name="proyecto1-connections-table",
            partition_key=dynamodb.Attribute(
                name="connectionId",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            time_to_live_attribute="ttl"
        )
        
        # Tabla DynamoDB para jobs de transcripción
        jobs_table = dynamodb.Table(
            self,
            "proyecto1-jobs-table",
            table_name="proyecto1-jobs-table",
            partition_key=dynamodb.Attribute(
                name="jobName",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        
        # ========== WEBSOCKET API ==========
        
        # WebSocket API
        websocket_api = apigwv2.WebSocketApi(
            self, "proyecto1-transcripcion-websocket",
            api_name="proyecto1-transcripcion-websocket",
            description="WebSocket API para notificaciones de transcripción en tiempo real"
        )
        
        # WebSocket Stage (crear antes de usar el endpoint)
        # Esto permite crear el stage del API Gateway aunque no tiene un uso en el resto del código.
        websocket_stage = apigwv2.WebSocketStage(
            self, "proyecto1-production-stage",
            web_socket_api=websocket_api,
            stage_name="prod",
            auto_deploy=True
        )

        # Crear un output con la URL del Websocket.
        # Este código es sólo para darle un uso a la variable websocket_stage creada antes
        CfnOutput(
            self, "WebSocketURL",
            value=f"wss://{websocket_api.api_id}.execute-api.{self.region}.amazonaws.com/{websocket_stage.stage_name}",
            description="WebSocket API URL"
        )
        
        # WebSocket endpoint URL (usar después del stage)
        websocket_endpoint = f"wss://{websocket_api.api_id}.execute-api.{self.region}.amazonaws.com/prod"
        
        # ========== LAMBDAS WEBSOCKET ==========
        
        # Variables de entorno comunes
        common_env = {
            "BUCKET": self.bucket.bucket_name,
            "CONNECTIONS_TABLE": connections_table.table_name,
            "JOBS_TABLE": jobs_table.table_name,
            "WEBSOCKET_API_ENDPOINT": websocket_endpoint
        }
        
        # Lambda para manejar conexiones WebSocket
        websocket_handler = _lambda.Function(
            self, 
            "proyecto1-websocket-handler",
            function_name="proyecto1-websocket-handler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("lambda/websocket"),
            handler="lambda_function.lambda_handler",
            environment=common_env,
            timeout=Duration.minutes(2),
            memory_size=256
        )
        
        # Lambda para manejar eventos de Transcribe/Bedrock
        event_handler = _lambda.Function(
            self,
            "proyecto1-event-handler",
            function_name="proyecto1-event-handler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("lambda/event_handler"),
            handler="lambda_function.lambda_handler",
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512
        )
        
        # ========== LAMBDAS PRINCIPALES ==========

        # Lambda para transcribir
        self.fn_transcribir = _lambda.Function(
            self, 
            "proyecto1-transcribir-audios",
            function_name="proyecto1-transcribir-audios",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/transcribir"),
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512,
        )

        # Lambda para formatear
        self.fn_formatear = _lambda.Function(
            self,
            "proyecto1-formatear-transcripcion",
            function_name="proyecto1-formatear-transcripcion",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/formatear"),
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512,
        )

        # Lambda para resumir
        self.fn_resumir = _lambda.Function(
            self,
            "proyecto1-resumir-transcripciones",
            function_name="proyecto1-resumir-transcripciones",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=_lambda.Code.from_asset("lambda/resumir"),
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512,
        )

        # ========== PERMISOS ==========
        
        # Permisos CloudWatch Logs
        cloudwatch_logs_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            resources=[f"arn:aws:logs:{self.region}:{self.account}:*"]
        )

        # Permisos DynamoDB
        for fn in [websocket_handler, event_handler, self.fn_transcribir, self.fn_formatear, self.fn_resumir]:
            connections_table.grant_read_write_data(fn)
            jobs_table.grant_read_write_data(fn)
            fn.add_to_role_policy(cloudwatch_logs_policy)

        # Permisos S3 mejorados
        s3_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:GetObjectMetadata",
                "s3:GetObjectTagging",
                "s3:PutObjectTagging",
                "s3:ListBucket"
            ],
            resources=[
                f"{self.bucket.bucket_arn}/*",
                f"{self.bucket.bucket_arn}"
            ]
        )

        for fn in [self.fn_transcribir, self.fn_formatear, self.fn_resumir, event_handler]:
            self.bucket.grant_read_write(fn)
            fn.add_to_role_policy(s3_policy)

        # Permisos mejorados para API Gateway Management
        websocket_management_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "execute-api:ManageConnections",
                "execute-api:Invoke"
            ],
            resources=[
                f"arn:aws:execute-api:{self.region}:{self.account}:{websocket_api.api_id}/*/*/*",
                f"arn:aws:execute-api:{self.region}:{self.account}:{websocket_api.api_id}/*/POST/@connections/*"
            ]
        )

        for fn in [websocket_handler, event_handler, self.fn_transcribir, self.fn_formatear, self.fn_resumir]:
            fn.add_to_role_policy(websocket_management_policy)

        # Permisos Transcribe (mantener existente)
        self.fn_transcribir.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "transcribe:StartTranscriptionJob",
                    "transcribe:GetTranscriptionJob",
                    "transcribe:ListTranscriptionJobs",
                ],
                resources=["*"],
            )
        )

        # Permisos Bedrock (mantener existente)
        self.fn_resumir.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                    "bedrock:ListFoundationModels",
                ],
                resources=["*"],
            )
        )

        # Permisos EventBridge
        event_handler.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "events:PutEvents",
                    "events:List*",
                    "events:Describe*"
                ],
                resources=["*"]
            )
        )
        
        # ========== RUTAS WEBSOCKET ==========
        
        # Integraciones WebSocket - ✅ CORREGIDO
        connect_integration = apigwv2_integrations.WebSocketLambdaIntegration(
            "ConnectIntegration",
            websocket_handler
        )
        
        disconnect_integration = apigwv2_integrations.WebSocketLambdaIntegration(
            "DisconnectIntegration",
            websocket_handler
        )
        
        # Rutas WebSocket
        websocket_api.add_route("$connect", integration=connect_integration)
        websocket_api.add_route("$disconnect", integration=disconnect_integration)
        
        # ========== EVENTBRIDGE RULES ==========
        
        transcribe_rule = events.Rule(
            self, "TranscribeEventRule",
            description="Captura eventos de cambio de estado de Transcribe",
            event_pattern=events.EventPattern(
                source=["aws.transcribe"],
                detail_type=["Transcribe Job State Change"]
            )
        )
        transcribe_rule.add_target(events_targets.LambdaFunction(event_handler))

        # ========== NOTIFICACIONES S3 ==========

        # Notificaciones S3 (crear después de que las lambdas existan)
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.fn_formatear),
            s3.NotificationKeyFilter(prefix=self.PFX_TRANSCRIPCIONES, suffix=".json"),
        )

        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.fn_resumir),
            s3.NotificationKeyFilter(prefix=self.PFX_TRANSCRIPCIONES_FMT, suffix=".txt"),
        )

        # Agregar esta nueva notificación junto a las existentes:
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.fn_transcribir),  # Función transcribir
            s3.NotificationKeyFilter(
                prefix=self.PFX_AUDIOS,     # Archivos subidos por usuarios
                suffix=".mp3"               # O el formato que uses para audios
            )
        )

        # ========== API GATEWAY REST ==========

        api = apigateway.RestApi(
            self,
            "proyecto1-transcripcion-api",
            rest_api_name="proyecto1-transcripcion-api",
            description="API REST para iniciar procesos de transcripción",
            deploy_options=apigateway.StageOptions(stage_name="prod"),
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS,
                allow_headers=["Content-Type", "Authorization"]
            )
        )
        
        # ========== OUTPUTS ==========

        CfnOutput(
            self, "WebSocketApiUrl",
            value=websocket_endpoint,
            description="WebSocket API URL para conexiones del frontend"
        )

        CfnOutput(
            self, "RestApiUrl",
            value=api.url,
            description="REST API URL para iniciar transcripciones"
        )

        CfnOutput(
            self, "BucketName",
            value=self.bucket.bucket_name,
            description="Nombre del bucket S3"
        )

        CfnOutput(
            self, "ConnectionsTableName",
            value=connections_table.table_name,
            description="Nombre de la tabla de conexiones WebSocket"
        )

        CfnOutput(
            self, "JobsTableName",
            value=jobs_table.table_name,
            description="Nombre de la tabla de jobs de transcripción"
        )