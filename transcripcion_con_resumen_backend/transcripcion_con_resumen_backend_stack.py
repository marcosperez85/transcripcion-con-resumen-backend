from aws_cdk import (
    Duration,
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_iam as iam,
    aws_apigateway as apigateway,
)

from constructs import Construct

class TranscripcionConResumenBackendStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "TranscripcionConResumenBackendQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )

        # Buckets existentes (o creación de nuevos bucket)
        bucket_general = s3.Bucket.from_bucket_name(self, "BucketGeneral", "transcripcion-con-resumen")

        # Lambda de transcripción
        lambda_transcribir = lambda_.Function(self, "proyecto1-transcribir-audios",
            function_name = "proyecto1-transcribir-audios",
            runtime = lambda_.Runtime.PYTHON_3_12,
            handler = "lambda_function.handler",
            code = lambda_.Code.from_asset("lambda/transcribir"),
            environment = {
                "BUCKET": bucket_general.bucket_name,
            },
            timeout = Duration.minutes(5),
            memory_size = 512
        )

        lambda_formatear = lambda_.Function(self, "proyecto1-formatear-transcripcion",
            function_name = "proyecto1-formatear-transcripcion",
            runtime = lambda_.Runtime.PYTHON_3_12,
            handler = "lambda_function.handler",
            code = lambda_.Code.from_asset("lambda/formatear"),
            environment = {
                "BUCKET": bucket_general.bucket_name,               
            },
            timeout = Duration.minutes(5),
            memory_size = 512
        )

        lambda_resumir = lambda_.Function(self, "proyecto1-resumir-transcripciones",
            function_name = "proyecto1-resumir-transcripciones",
            runtime = lambda_.Runtime.PYTHON_3_12,
            handler = "lambda_function.handler",
            code = lambda_.Code.from_asset("lambda/formatear"),
            environment = {
                "BUCKET": bucket_general.bucket_name,
            },
            timeout = Duration.minutes(5),
            memory_size = 512
        )

        api = apigateway.RestApi(self, "TranscripcionAPI")
        transcribir_integration = apigateway.LambdaIntegration(lambda_transcribir)
        api.root.add_resource("transcribir").add_method("POST", transcribir_integration)

        # Permisos para leer y escribir en el bucket
        bucket_general.grant_read_write(lambda_transcribir)
        bucket_general.grant_read_write(lambda_formatear)
        bucket_general.grant_read_write(lambda_resumir)
