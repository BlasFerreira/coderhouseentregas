import boto3
import json

def lambda_handler(event, context):
    # Cliente SNS
    sns_client = boto3.client('sns', region_name='us-east-1')

    # ARN de tu tema SNS
    topic_arn = 'arn:aws:sns:us-east-1:141852926562:entregafinal'

    for record in event['Records']:
        # Extracción del mensaje de la cola SQS
        message_json = json.loads(record['body'])
        subject = message_json.get('subject', 'No Subject')
        message = message_json.get('message', '')

        # Publicación del mensaje en el tema SNS
        response = sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
            Subject=subject
        )

    return response
