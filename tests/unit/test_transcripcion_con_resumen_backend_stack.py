import aws_cdk as core
import aws_cdk.assertions as assertions

from transcripcion_con_resumen_backend.transcripcion_con_resumen_backend_stack import TranscripcionConResumenBackendStack

# example tests. To run these tests, uncomment this file along with the example
# resource in transcripcion_con_resumen_backend/transcripcion_con_resumen_backend_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = TranscripcionConResumenBackendStack(app, "transcripcion-con-resumen-backend")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
