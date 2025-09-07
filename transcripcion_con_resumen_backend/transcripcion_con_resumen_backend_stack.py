from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_iam as iam,
    aws_apigateway as apigateway,
    aws_s3_notifications as s3n,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct

# Días en los que se borran automáticamente los objetos alojados en el bucket de backend
dias_de_expiracion = 3


class TranscripcionConResumenBackendStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1 Bucket único (usa prefijos). Agrego nombre de cuenta y de región para evitar nombres hardcodeados
        bucket_name = f"transcripcion-con-resumen-backend-{self.account}-{self.region}"
        self.bucket = s3.Bucket(
            self,
            "BucketGeneral",
            bucket_name=bucket_name,
            cors=[
                s3.CorsRule(
                    allowed_methods=[
                        s3.HttpMethods.GET,
                        s3.HttpMethods.POST,
                        s3.HttpMethods.PUT,
                    ],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3000,
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,  # elimina los objetos antes de borrar el bucket
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(dias_de_expiracion))
            ],
        )

        # Prefijos (solo constantes para usar en filtros/keys)
        self.PFX_AUDIOS = "audios/"
        self.PFX_TRANSCRIPCIONES = "transcripciones/"
        self.PFX_TRANSCRIPCIONES_FMT = "transcripciones-formateadas/"
        self.PFX_RESUMENES = "resumenes/"
        self.PFX_STATIC = "static/"

        # 2) Lambdas (guardar referencias)
        common_env = {"BUCKET": self.bucket.bucket_name}

        self.fn_transcribir = lambda_.Function(
            self,
            "proyecto1-transcribir-audios",
            function_name="proyecto1-transcribir-audios",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("lambda/transcribir"),
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512,
        )

        self.fn_formatear = lambda_.Function(
            self,
            "proyecto1-formatear-transcripcion",
            function_name="proyecto1-formatear-transcripcion",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("lambda/formatear"),
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512,
        )

        self.fn_resumir = lambda_.Function(
            self,
            "proyecto1-resumir-transcripciones",
            function_name="proyecto1-resumir-transcripciones",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="lambda_function.lambda_handler",
            code=lambda_.Code.from_asset("lambda/resumir"),
            environment=common_env,
            timeout=Duration.minutes(5),
            memory_size=512,
        )

        # 3 Permisos de bucket más específicos
        # Transcribir: lee audios, escribe en transcripciones
        self.fn_transcribir.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[
                    f"{self.bucket.bucket_arn}/{self.PFX_AUDIOS}*",
                    f"{self.bucket.bucket_arn}/{self.PFX_TRANSCRIPCIONES_FMT}*",  # Para leer transcripciones formateadas
                    f"{self.bucket.bucket_arn}/{self.PFX_RESUMENES}*",           # Para leer resúmenes
                ],
            )
        )
        self.fn_transcribir.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"{self.bucket.bucket_arn}/{self.PFX_TRANSCRIPCIONES}*"],
            )
        )

        # Formatear: lee transcripciones, escribe formateadas
        self.fn_formatear.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{self.bucket.bucket_arn}/{self.PFX_TRANSCRIPCIONES}*"],
            )
        )
        self.fn_formatear.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"{self.bucket.bucket_arn}/{self.PFX_TRANSCRIPCIONES_FMT}*"],
            )
        )

        # Resumir: lee formateadas, escribe resúmenes
        self.fn_resumir.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{self.bucket.bucket_arn}/{self.PFX_TRANSCRIPCIONES_FMT}*"],
            )
        )
        self.fn_resumir.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"{self.bucket.bucket_arn}/{self.PFX_RESUMENES}*"],
            )
        )

        # for fn in [self.fn_transcribir, self.fn_formatear, self.fn_resumir]:
        #     self.bucket.grant_read_write(fn)

        # 4 Permisos específicos de servicio
        # Transcribe para la Lambda de transcribir
        self.fn_transcribir.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "transcribe:StartTranscriptionJob",
                    "transcribe:GetTranscriptionJob",
                    "transcribe:ListTranscriptionJobs",
                ],
                resources=[
                    f"arn:aws:transcribe:{self.region}:{self.account}:transcription-job/*"
                ],
            )
        )

        # Bedrock para la Lambda de resumir (restringí al ARN del modelo cuando lo tengas fijo)
        self.fn_resumir.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream",
                ],
                # Especifico sólo los modelos que realmente uso
                resources=[
                    f"arn:aws:bedrock:{self.region}::foundation-model/amazon.titan-text-express-v1",
                ],
            )
        )

        # 5 Notificaciones S3 → Lambdas (prefijos correctos)
        # Cuando aparece un .json en transcripciones/ => formatear
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.fn_formatear),
            s3.NotificationKeyFilter(prefix=self.PFX_TRANSCRIPCIONES, suffix=".json"),
        )

        # Cuando aparece un .txt en transcripciones-formateadas/ => resumir
        self.bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(self.fn_resumir),
            s3.NotificationKeyFilter(
                prefix=self.PFX_TRANSCRIPCIONES_FMT, suffix=".txt"
            ),
        )

        # 6 API Gateway (solo para kick-off de transcripción)
        api = apigateway.RestApi(
            self,
            "TranscripcionApi",
            rest_api_name="Transcripcion API",
            deploy_options=apigateway.StageOptions(stage_name="prod"),
        )
        transcribir_res = api.root.add_resource("transcribir")

        # Método POST
        transcribir_res.add_method(
            "POST",
            apigateway.LambdaIntegration(self.fn_transcribir, proxy=True),
            method_responses=[
                apigateway.MethodResponse(
                    status_code="200",
                    response_parameters={
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                    },
                ),
            ],
        )

        # Método OPTIONS (preflight CORS)
        transcribir_res.add_method(
            "OPTIONS",
            apigateway.MockIntegration(
                integration_responses=[
                    {
                        "statusCode": "200",
                        "responseParameters": {
                            "method.response.header.Access-Control-Allow-Headers": "'Content-Type'",
                            "method.response.header.Access-Control-Allow-Origin": "'*'",
                            "method.response.header.Access-Control-Allow-Methods": "'OPTIONS,POST'",
                        },
                        "responseTemplates": {"application/json": "{}"},
                    }
                ],
                passthrough_behavior=apigateway.PassthroughBehavior.NEVER,
                request_templates={"application/json": '{"statusCode": 200}'},
            ),
            method_responses=[
                {
                    "statusCode": "200",
                    "responseParameters": {
                        "method.response.header.Access-Control-Allow-Headers": True,
                        "method.response.header.Access-Control-Allow-Origin": True,
                        "method.response.header.Access-Control-Allow-Methods": True,
                    },
                }
            ],
        )

        # Declaro outputs para el deploy
        CfnOutput(self, "BackendBucketName", value=self.bucket.bucket_name)
