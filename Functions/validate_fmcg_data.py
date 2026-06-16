import boto3
import csv
import io
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):

    try:

        # Get S3 event details
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']

        print(f"Bucket: {bucket}")
        print(f"Key: {key}")

        # Process ONLY bronze files
        if not key.startswith('bronze-raw-events/'):
            print("File not in bronze layer. Skipping.")
            return {
                'statusCode': 200,
                'message': 'Skipped'
            }

        # Read Bronze File
        response = s3.get_object(
            Bucket=bucket,
            Key=key
        )

        csv_content = response['Body'].read().decode('utf-8')

        reader = csv.DictReader(
            io.StringIO(csv_content)
        )

        print("Headers Found:")
        print(reader.fieldnames)

        valid_rows = []
        invalid_rows = []

        processing_timestamp = datetime.utcnow().strftime(
            '%Y-%m-%d %H:%M:%S'
        )

        for row in reader:

            try:

                cleaned = {}

                for k, v in row.items():
                    cleaned[str(k).strip()] = (
                        str(v).strip()
                        if v is not None
                        else ""
                    )

                price_unit = float(cleaned['price_unit'])
                units_sold = float(cleaned['units_sold'])
                delivered_qty = float(cleaned['delivered_qty'])
                stock_available = float(cleaned['stock_available'])
                delivery_days = float(cleaned['delivery_days'])

                if (
                    price_unit > 0 and
                    units_sold >= 0 and
                    delivered_qty >= 0 and
                    stock_available >= 0 and
                    delivery_days >= 0
                ):

                    cleaned['revenue'] = round(
                        price_unit * units_sold,
                        2
                    )

                    cleaned['inventory_gap'] = round(
                        delivered_qty - units_sold,
                        2
                    )

                    cleaned['processing_timestamp'] = (
                        processing_timestamp
                    )

                    valid_rows.append(cleaned)

                else:

                    cleaned['error_reason'] = (
                        'Validation Failed'
                    )

                    invalid_rows.append(cleaned)

            except Exception as e:

                cleaned['error_reason'] = str(e)
                invalid_rows.append(cleaned)

        print(f"Valid Rows: {len(valid_rows)}")
        print(f"Invalid Rows: {len(invalid_rows)}")

        # Save Silver Layer
        if len(valid_rows) > 0:

            silver_buffer = io.StringIO()

            writer = csv.DictWriter(
                silver_buffer,
                fieldnames=valid_rows[0].keys()
            )

            writer.writeheader()
            writer.writerows(valid_rows)

            s3.put_object(
                Bucket=bucket,
                Key='silver-validated-events/validated.csv',
                Body=silver_buffer.getvalue()
            )

            print(
                "Silver file created successfully"
            )

        # Save Quarantine Layer
        if len(invalid_rows) > 0:

            quarantine_buffer = io.StringIO()

            writer = csv.DictWriter(
                quarantine_buffer,
                fieldnames=invalid_rows[0].keys()
            )

            writer.writeheader()
            writer.writerows(invalid_rows)

            s3.put_object(
                Bucket=bucket,
                Key='quarantine/bad_records.csv',
                Body=quarantine_buffer.getvalue()
            )

            print(
                "Quarantine file created successfully"
            )

        return {
            'statusCode': 200,
            'valid_records': len(valid_rows),
            'invalid_records': len(invalid_rows)
        }

    except Exception as e:

        print("ERROR:", str(e))

        return {
            'statusCode': 500,
            'error': str(e)
        }