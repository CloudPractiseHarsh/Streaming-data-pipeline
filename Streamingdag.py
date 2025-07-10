from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateTopicOperator,
    PubSubCreateSubscriptionOperator
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCheckOperator
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSFileTransformOperator
)
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator, BeamRunnerType
from airflow.providers.google.cloud.sensors.dataflow import (
    DataflowJobStatusSensor
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import json

# Configuration
PROJECT_ID = "gen-lang-client-0351304067"
REGION = "us-central1"
BUCKET_NAME = "us-central1-dev-c37c360e-bucket"
PUBSUB_TOPIC = "streaming-data-topic"
PUBSUB_SUBSCRIPTION = "streaming-data-subscription"
BIGQUERY_DATASET = "streaming_dataset"
BIGQUERY_TABLE = "streaming_table"
DATAFLOW_JOB_NAME = "streaming-pipeline-dataflow"
PIPELINE_FILE = "streaming_pipeline.py"

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'complete_streaming_pipeline',
    default_args=default_args,
    description='Complete streaming pipeline setup and deployment',
    schedule_interval='@once',  # Run once or trigger manually
    catchup=False,
    tags=['streaming', 'pubsub', 'dataflow', 'bigquery', 'complete'],
)

# Task 1: Enable required APIs
enable_apis = BashOperator(
    task_id='enable_required_apis',
    bash_command=f"""
    gcloud services enable dataflow.googleapis.com --project={PROJECT_ID}
    gcloud services enable bigquery.googleapis.com --project={PROJECT_ID}
    gcloud services enable pubsub.googleapis.com --project={PROJECT_ID}
    gcloud services enable storage.googleapis.com --project={PROJECT_ID}
    """,
    dag=dag,
)

# # Task 2: Create GCS bucket for staging (if not exists)
# create_gcs_bucket = GCSCreateBucketOperator(
#     task_id='create_gcs_bucket',
#     bucket_name=BUCKET_NAME,
#     project_id=PROJECT_ID,
#     location=REGION,
#     dag=dag,
# )

# Task 3: Create Pub/Sub topic
create_pubsub_topic = PubSubCreateTopicOperator(
    task_id='create_pubsub_topic',
    project_id=PROJECT_ID,
    topic=PUBSUB_TOPIC,
    dag=dag,
)

# Task 4: Create Pub/Sub subscription
create_pubsub_subscription = PubSubCreateSubscriptionOperator(
    task_id='create_pubsub_subscription',
    project_id=PROJECT_ID,
    topic=PUBSUB_TOPIC,
    subscription=PUBSUB_SUBSCRIPTION,
    dag=dag,
)

# Task 5: Create BigQuery dataset
create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bigquery_dataset',
    project_id=PROJECT_ID,
    dataset_id=BIGQUERY_DATASET,
    location=REGION,
    dag=dag,
)

# Task 6: Create BigQuery table
create_bigquery_table = BigQueryCreateEmptyTableOperator(
    task_id='create_bigquery_table',
    project_id=PROJECT_ID,
    dataset_id=BIGQUERY_DATASET,
    table_id=BIGQUERY_TABLE,
    schema_fields=[
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "message", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "value", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "processed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        # Optional fields
        {"name": "source", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ip_address", "type": "STRING", "mode": "NULLABLE"},
        {"name": "user_agent", "type": "STRING", "mode": "NULLABLE"},
        {"name": "page", "type": "STRING", "mode": "NULLABLE"},
        {"name": "referrer", "type": "STRING", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "device_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "location", "type": "STRING", "mode": "NULLABLE"},
        # Enriched fields
        {"name": "message_length", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "event_hour", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "data_quality_score", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    dag=dag,
)

# Task 7: Upload Dataflow pipeline code to GCS
def upload_pipeline_code(**context):
    """Upload the Dataflow pipeline code to GCS."""
    gcs_hook = GCSHook()
    
    # Pipeline code content (from our optimized pipeline)
    pipeline_code = '''
import argparse
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteDisposition


class ParsePubSubMessage(beam.DoFn):
    """Parse JSON messages from Pub/Sub and handle errors gracefully."""
    
    def process(self, element):
        """Process each message from Pub/Sub."""
        try:
            # Handle both string and bytes
            if isinstance(element, bytes):
                message_str = element.decode('utf-8')
            else:
                message_str = element
            
            # Parse JSON
            data = json.loads(message_str)
            
            # Validate that we have the minimum required fields
            required_fields = ['id', 'timestamp', 'message', 'user_id', 'event_type']
            
            # Check if all required fields are present
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                logging.warning(f"Missing required fields: {missing_fields}")
                return  # Skip this message
            
            # Add processing timestamp
            data['processed_at'] = datetime.utcnow().isoformat()
            
            # Ensure all fields exist with proper types
            cleaned_data = {
                'id': str(data.get('id', '')),
                'timestamp': str(data.get('timestamp', '')),
                'message': str(data.get('message', '')),
                'user_id': str(data.get('user_id', '')),
                'event_type': str(data.get('event_type', '')),
                'value': float(data.get('value', 0.0)),
                'processed_at': data['processed_at']
            }
            
            # Add optional fields if they exist
            optional_fields = ['source', 'ip_address', 'user_agent', 'page', 'referrer', 
                             'product_id', 'currency', 'device_id', 'location']
            
            for field in optional_fields:
                if field in data:
                    cleaned_data[field] = str(data[field])
            
            # Add enrichments
            cleaned_data['message_length'] = len(cleaned_data.get('message', ''))
            try:
                cleaned_data['event_hour'] = datetime.fromisoformat(
                    cleaned_data['timestamp'].replace('Z', '+00:00')
                ).hour
            except:
                cleaned_data['event_hour'] = 0
            
            # Simple data quality score
            score = 0
            if cleaned_data.get('user_id') and cleaned_data['user_id'] != 'unknown':
                score += 25
            if cleaned_data.get('message'):
                score += 25
            if cleaned_data.get('value', 0) > 0:
                score += 25
            if cleaned_data.get('timestamp'):
                score += 25
            
            cleaned_data['data_quality_score'] = score
            
            # Filter low quality data
            if score >= 50:
                yield cleaned_data
            else:
                logging.warning(f"Filtered low quality message: {cleaned_data}")
            
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error: {e}. Message: {element}")
        except Exception as e:
            logging.error(f"Error processing message: {e}. Message: {element}")


def create_bigquery_schema():
    """Create BigQuery table schema."""
    return {
        'fields': [
            {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'message', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'value', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'processed_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ip_address', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'user_agent', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'page', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'referrer', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'product_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'currency', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'device_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'message_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'event_hour', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'data_quality_score', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        ]
    }


def run_pipeline(argv=None):
    """Run the streaming pipeline."""
    parser = argparse.ArgumentParser()
    
    # Required arguments
    parser.add_argument('--project_id', required=True, help='Google Cloud Project ID')
    parser.add_argument('--pubsub_subscription', required=True, help='Pub/Sub subscription')
    parser.add_argument('--bigquery_table', required=True, help='BigQuery table')
    parser.add_argument('--temp_location', required=True, help='GCS temp location')
    parser.add_argument('--staging_location', required=True, help='GCS staging location')
    parser.add_argument('--window_size', type=int, default=60, help='Window size in seconds')
    
    known_args, pipeline_args = parser.parse_known_args(argv)
    
    # Set up pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    
    # Create the pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        
        # Read from Pub/Sub
        raw_messages = (
            pipeline
            | 'Read from Pub/Sub' >> ReadFromPubSub(
                subscription=known_args.pubsub_subscription,
                with_attributes=False
            )
        )
        
        # Parse and process messages
        processed_messages = (
            raw_messages
            | 'Parse and Process Messages' >> beam.ParDo(ParsePubSubMessage())
        )
        
        # Apply windowing
        windowed_messages = (
            processed_messages
            | 'Apply Fixed Windows' >> beam.WindowInto(
                window.FixedWindows(known_args.window_size)
            )
        )
        
        # Write to BigQuery
        _ = (
            windowed_messages
            | 'Write to BigQuery' >> WriteToBigQuery(
                table=known_args.bigquery_table,
                schema=create_bigquery_schema(),
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=WriteDisposition.WRITE_APPEND,
                additional_bq_parameters={
                    'ignoreUnknownValues': True,
                    'autodetect': False
                }
            )
        )
    
    logging.info("Pipeline completed successfully")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()
'''
    
    # Upload the code to GCS
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=f'dataflow/{PIPELINE_FILE}',
        data=pipeline_code.encode('utf-8'),
        mime_type='text/x-python'
    )
    

upload_pipeline_code_task = PythonOperator(
    task_id='upload_pipeline_code',
    python_callable=upload_pipeline_code,
    dag=dag,
)

# Task 8: Deploy Dataflow streaming job
deploy_dataflow_job = BeamRunPythonPipelineOperator(
    task_id='deploy_dataflow_streaming_job',
    py_file=f'gs://{BUCKET_NAME}/dataflow/{PIPELINE_FILE}',
    pipeline_options={
        'project': PROJECT_ID,  # Use 'project' instead of 'project_id'
        'pubsub_subscription': f'projects/{PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION}',
        'bigquery_table': f'{PROJECT_ID}:{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
        'temp_location': f'gs://{BUCKET_NAME}/temp/',
        'staging_location': f'gs://{BUCKET_NAME}/staging/',
        'window_size': '60',  # Add window_size (in seconds) as required by the pipeline script
        'region': REGION,  # Specify the region for Dataflow
        'job_name': DATAFLOW_JOB_NAME,  # Set the Dataflow job name
    },
    gcp_conn_id='google_cloud_default',
    py_interpreter='python3',
    dag=dag,
)

# Task 9: Monitor Dataflow job startup
monitor_dataflow_startup = DataflowJobStatusSensor(
    task_id='monitor_dataflow_startup',
    project_id=PROJECT_ID,
    location=REGION,
    job_id=DATAFLOW_JOB_NAME,
    expected_statuses={'JOB_STATE_RUNNING'},
    timeout=600,
    poke_interval=30,
    dag=dag,
)

# Task 10: Send test messages to verify pipeline
def send_test_messages(**context):
    """Send test messages to Pub/Sub to verify the pipeline works."""
    pubsub_hook = PubSubHook()
    
    test_messages = [
        {
            "id": "test-dag-001",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "message": "Test message from DAG",
            "user_id": "dag_test_user",
            "event_type": "test",
            "value": 1.0,
            "source": "airflow_dag"
        },
        {
            "id": "test-dag-002",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "message": "Another test message from DAG",
            "user_id": "dag_test_user_2",
            "event_type": "test_purchase",
            "value": 99.99,
            "source": "airflow_dag",
            "product_id": "test_product_123"
        }
    ]
    
    for message in test_messages:
        pubsub_hook.publish(
            project_id=PROJECT_ID,
            topic=PUBSUB_TOPIC,
            messages=[json.dumps(message)]
        )
    
    logging.info(f"Sent {len(test_messages)} test messages to {PUBSUB_TOPIC}")

send_test_messages_task = PythonOperator(
    task_id='send_test_messages',
    python_callable=send_test_messages,
    dag=dag,
)

# Task 11: Verify data in BigQuery
verify_bigquery_data = BigQueryCheckOperator(
    task_id='verify_bigquery_data',
    sql=f"""
    SELECT COUNT(*) as record_count 
    FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}` 
    WHERE source = 'airflow_dag' 
    AND processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
    """,
    use_legacy_sql=False,
    dag=dag,
)

# Task 12: Pipeline health check
def pipeline_health_check(**context):
    """Check overall pipeline health."""
    bq_hook = BigQueryHook()
    
    # Check recent data
    query = f"""
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT event_type) as event_types,
        MAX(processed_at) as latest_processed
    FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
    WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """
    
    results = bq_hook.get_pandas_df(query)
    
    logging.info("Pipeline Health Check Results:")
    logging.info(f"Total records in last hour: {results['total_records'].iloc[0]}")
    logging.info(f"Unique users: {results['unique_users'].iloc[0]}")
    logging.info(f"Event types: {results['event_types'].iloc[0]}")
    logging.info(f"Latest processed: {results['latest_processed'].iloc[0]}")
    
    # Store results in XCom for potential alerts
    return {
        'total_records': int(results['total_records'].iloc[0]),
        'unique_users': int(results['unique_users'].iloc[0]),
        'event_types': int(results['event_types'].iloc[0]),
        'latest_processed': str(results['latest_processed'].iloc[0])
    }

pipeline_health_check_task = PythonOperator(
    task_id='pipeline_health_check',
    python_callable=pipeline_health_check,
    dag=dag,
)

# Task 13: Pipeline completion notification
pipeline_complete = DummyOperator(
    task_id='pipeline_deployment_complete',
    dag=dag,
)

# Define task dependencies
enable_apis >> [ create_pubsub_topic, create_bigquery_dataset]

create_pubsub_topic >> create_pubsub_subscription

create_bigquery_dataset >> create_bigquery_table

[create_pubsub_subscription, create_bigquery_table] >> upload_pipeline_code_task

upload_pipeline_code_task >> deploy_dataflow_job

deploy_dataflow_job >> monitor_dataflow_startup

monitor_dataflow_startup >> send_test_messages_task

send_test_messages_task >> verify_bigquery_data

verify_bigquery_data >> pipeline_health_check_task

pipeline_health_check_task >> pipeline_complete
