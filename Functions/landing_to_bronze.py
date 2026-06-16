import boto3
import urllib.parse

s3 = boto3.client('s3')

def lambda_handler(event, context):

    print("EVENT:", event)

    bucket = event['Records'][0]['s3']['bucket']['name']

    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key']
    )

    print("Bucket:", bucket)
    print("Key:", key)

    filename = key.split('/')[-1]

    s3.copy_object(
        Bucket=bucket,
        CopySource={
            'Bucket': bucket,
            'Key': key
        },
        Key=f'bronze-raw-events/{filename}'
    )

    print("Copied to bronze-raw-events")

    return {
        'statusCode': 200
    }