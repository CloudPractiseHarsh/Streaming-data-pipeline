# Streaming Data Pipeline with Apache Airflow and Google Cloud

A complete real-time streaming data pipeline that ingests data through Google Cloud Pub/Sub, processes it with Apache Beam on Google Cloud Dataflow, and stores results in BigQuery. Includes a Streamlit application for data generation and monitoring.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────┐
│ Streamlit App   │───▶│ Pub/Sub      │───▶│ Dataflow        │───▶│ BigQuery    │
│ (Data Producer) │    │ (Message     │    │ (Apache Beam    │    │ (Data       │
│                 │    │  Queue)      │    │  Processing)    │    │  Warehouse) │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────┘
                                ▲                    ▲
                                │                    │
                       ┌────────┴────────┐  ┌───────┴────────┐
                       │ Apache Airflow  │  │ Google Cloud   │
                       │ (Orchestration) │  │ Storage (GCS)  │
                       └─────────────────┘  └────────────────┘
```

## 📁 Project Structure

```
streaming-pipeline/
├── dags/
│   └── complete_streaming_pipeline.py    # Main Airflow DAG
├── apps/
│   └── streamlit_publisher.py           # Streamlit data generator
├── dataflow/
│   └── streaming_pipeline.py            # Apache Beam pipeline (auto-generated)
├── requirements.txt
├── docker-compose.yml                   # Optional: Local Airflow setup
└── README.md
```

## 🚀 Quick Start

### Prerequisites

- Google Cloud Platform account with billing enabled
- Python 3.8+
- Apache Airflow 2.0+
- Google Cloud SDK (gcloud CLI)

### 1. Google Cloud Setup

```bash
# Set your project ID
export PROJECT_ID="your-project-id"
gcloud config set project $PROJECT_ID

# Enable required APIs
gcloud services enable dataflow.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable storage.googleapis.com

# Create a service account for authentication
gcloud iam service-accounts create streaming-pipeline \
    --display-name="Streaming Pipeline Service Account"

# Grant necessary permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:streaming-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/dataflow.developer"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:streaming-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/pubsub.editor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:streaming-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:streaming-pipeline@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

# Create and download service account key
gcloud iam service-accounts keys create ~/streaming-pipeline-key.json \
    --iam-account=streaming-pipeline@$PROJECT_ID.iam.gserviceaccount.com

# Set the environment variable
export GOOGLE_APPLICATION_CREDENTIALS=~/streaming-pipeline-key.json
```

### 2. Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd streaming-pipeline

# Install Python dependencies
pip install -r requirements.txt

# For Airflow (if not already installed)
pip install apache-airflow[google]
```

### 3. Configuration

#### Update Airflow DAG Configuration

Edit `dags/complete_streaming_pipeline.py`:

```python
# Configuration
PROJECT_ID = "your-project-id"                    # Replace with your project ID
REGION = "us-central1"                            # Your preferred region
BUCKET_NAME = "your-bucket-name"                  # Replace with your bucket name
PUBSUB_TOPIC = "streaming-data-topic"
PUBSUB_SUBSCRIPTION = "streaming-data-subscription"
BIGQUERY_DATASET = "streaming_dataset"
BIGQUERY_TABLE = "streaming_table"
```

#### Update Streamlit App Configuration

Edit `apps/streamlit_publisher.py`:

```python
# Configuration
PROJECT_ID = "your-project-id"        # Replace with your project ID
TOPIC_NAME = "streaming-data-topic"   # Match the topic name from DAG
```

## 🔧 Usage

### 1. Deploy the Pipeline with Airflow

```bash
# Start Airflow (if using local installation)
airflow webserver --port 8080 &
airflow scheduler &

# Access Airflow UI at http://localhost:8080
# Username/Password: admin/admin (default)

# Manually trigger the DAG
airflow dags trigger complete_streaming_pipeline
```

The DAG will automatically:
- ✅ Enable required Google Cloud APIs
- ✅ Create Pub/Sub topic and subscription
- ✅ Create BigQuery dataset and table
- ✅ Upload and deploy Dataflow pipeline
- ✅ Start streaming job
- ✅ Send test messages
- ✅ Verify data flow

### 2. Generate Data with Streamlit App

```bash
# Run the Streamlit application
streamlit run apps/streamlit_publisher.py

# Access the app at http://localhost:8501
```

#### Streamlit Features:

- **📝 Manual Events**: Create and publish individual events
- **🔄 Auto Publishing**: Automatically generate events at specified rates
- **📊 Analytics**: Visualize published events
- **📈 Monitoring**: Monitor pipeline health and metrics
- **⚙️ Settings**: Configure app settings and export data

### 3. Monitor the Pipeline

#### Airflow Monitoring
- Access Airflow UI: `http://localhost:8080`
- Monitor DAG runs and task status
- Check logs for debugging

#### Google Cloud Console
- **Dataflow**: Monitor streaming job status and metrics
- **Pub/Sub**: Check topic and subscription metrics
- **BigQuery**: Query processed data
- **Cloud Logging**: View detailed logs

#### Query Data in BigQuery

```sql
-- Check recent data
SELECT 
    event_type,
    COUNT(*) as event_count,
    AVG(value) as avg_value,
    MAX(processed_at) as latest_processed
FROM `your-project-id.streaming_dataset.streaming_table`
WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY event_type
ORDER BY event_count DESC;

-- Data quality analysis
SELECT 
    data_quality_score,
    COUNT(*) as message_count,
    AVG(message_length) as avg_message_length
FROM `your-project-id.streaming_dataset.streaming_table`
WHERE processed_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY data_quality_score
ORDER BY data_quality_score DESC;
```

## 📊 Data Schema

The pipeline processes events with the following schema:

| Field | Type | Mode | Description |
|-------|------|------|-------------|
| `id` | STRING | REQUIRED | Unique event identifier |
| `timestamp` | TIMESTAMP | REQUIRED | Event timestamp |
| `message` | STRING | NULLABLE | Event message/description |
| `user_id` | STRING | NULLABLE | User identifier |
| `event_type` | STRING | NULLABLE | Type of event (login, purchase, etc.) |
| `value` | FLOAT | NULLABLE | Numeric value associated with event |
| `processed_at` | TIMESTAMP | REQUIRED | Pipeline processing timestamp |
| `source` | STRING | NULLABLE | Event source system |
| `ip_address` | STRING | NULLABLE | User IP address |
| `user_agent` | STRING | NULLABLE | User agent string |
| `page` | STRING | NULLABLE | Page URL for web events |
| `referrer` | STRING | NULLABLE | Referrer URL |
| `product_id` | STRING | NULLABLE | Product identifier for commerce events |
| `currency` | STRING | NULLABLE | Currency code |
| `device_id` | STRING | NULLABLE | Device identifier |
| `location` | STRING | NULLABLE | Geographic location |
| `message_length` | INTEGER | NULLABLE | Length of message field |
| `event_hour` | INTEGER | NULLABLE | Hour of day (0-23) |
| `data_quality_score` | INTEGER | NULLABLE | Data quality score (0-100) |

## 🔍 Troubleshooting

### Common Issues

#### Authentication Errors
```bash
# Verify service account key is set
echo $GOOGLE_APPLICATION_CREDENTIALS

# Test authentication
gcloud auth application-default print-access-token
```

#### Dataflow Job Fails
- Check Google Cloud Console > Dataflow for job details
- Verify IAM permissions for service account
- Check Airflow logs for detailed error messages

#### Pub/Sub Connection Issues
- Verify topic exists: `gcloud pubsub topics list`
- Check subscription: `gcloud pubsub subscriptions list`
- Ensure service account has Pub/Sub permissions

#### BigQuery Schema Mismatch
```sql
-- Check table schema
SELECT column_name, data_type, is_nullable
FROM `your-project-id.streaming_dataset.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'streaming_table';
```

### Logs and Debugging

```bash
# Airflow task logs
airflow tasks log complete_streaming_pipeline <task_id> <execution_date>

# Google Cloud Logging
gcloud logging read "resource.type=dataflow_job" --limit=50

# Pub/Sub monitoring
gcloud pubsub topics describe streaming-data-topic
gcloud pubsub subscriptions describe streaming-data-subscription
```

## 🚀 Advanced Configuration

### Custom Event Types

Add custom event types by modifying the Streamlit app:

```python
# In streamlit_publisher.py
event_types = ["login", "logout", "purchase", "page_view", "search", "add_to_cart", "your_custom_event"]
```

### Scaling Configuration

Modify Dataflow pipeline options in the DAG:

```python
# In complete_streaming_pipeline.py
pipeline_options={
    'project': PROJECT_ID,
    'region': REGION,
    'num_workers': 2,           # Adjust worker count
    'max_num_workers': 10,      # Auto-scaling limit
    'machine_type': 'n1-standard-2',  # Machine type
    'window_size': '60',        # Window size in seconds
}
```

### Data Retention

Configure BigQuery table expiration:

```python
# Add to BigQuery table creation
table_resource = {
    'expirationTime': str(int(time.time() + 30 * 24 * 60 * 60) * 1000)  # 30 days
}
```

## 📈 Monitoring and Alerting

### Set up Cloud Monitoring Alerts

```bash
# Create alert policy for Dataflow job status
gcloud alpha monitoring policies create --policy-from-file=monitoring-policy.yaml
```

### Custom Metrics

Add custom metrics to the Dataflow pipeline:

```python
# In the Apache Beam pipeline
from apache_beam.metrics import Metrics

# Define counters
processed_records = Metrics.counter('pipeline', 'processed_records')
error_records = Metrics.counter('pipeline', 'error_records')

# Increment in DoFn
processed_records.inc()
```

## 🛡️ Security Best Practices

1. **IAM Principle of Least Privilege**: Grant only necessary permissions
2. **Service Account Key Management**: Rotate keys regularly
3. **Network Security**: Use VPC and firewall rules if needed
4. **Data Encryption**: Enable encryption at rest and in transit
5. **Audit Logging**: Enable Cloud Audit Logs for compliance

## 📝 Cost Optimization

- **Dataflow**: Use preemptible instances for cost savings
- **BigQuery**: Partition tables by date for query efficiency
- **Pub/Sub**: Set appropriate message retention policies
- **GCS**: Use lifecycle policies for object management

