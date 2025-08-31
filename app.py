#!/usr/bin/env python3
import os
import aws_cdk as cdk

from transcripcion_con_resumen_backend.transcripcion_con_resumen_backend_stack import TranscripcionConResumenBackendStack

app = cdk.App()

TranscripcionConResumenBackendStack(
    app, 
    'TranscripcionConResumenBackendStack',
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region=os.getenv('CDK_DEFAULT_REGION', 'us-east-1')
    )
)

app.synth()