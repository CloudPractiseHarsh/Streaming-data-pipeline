import streamlit as st
import json
import uuid
import random
import time
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, Any, List

# Configuration
PROJECT_ID = "YOUR-PROJECT_ID"
TOPIC_NAME = "YOUR-TOPIC-NAME"

# Initialize Pub/Sub client
@st.cache_resource
def init_pubsub_client():
    """Initialize Pub/Sub client with caching."""
    try:
        # Check for service account key file
        import os
        from google.oauth2 import service_account
        
        credentials = None
        
        # Try to use service account key if available
        if os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            credentials = service_account.Credentials.from_service_account_file(
                os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            )
            st.info("🔑 Using service account credentials")
        else:
            st.info("🔑 Using application default credentials")
        
        # Initialize client with credentials
        if credentials:
            publisher = pubsub_v1.PublisherClient(credentials=credentials)
        else:
            publisher = pubsub_v1.PublisherClient()
        
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
        
        # Test the connection by trying to get topic info
        try:
            publisher.get_topic(request={"topic": topic_path})
            st.success("✅ Successfully connected to Pub/Sub topic!")
        except Exception as topic_error:
            st.error(f"❌ Topic '{TOPIC_NAME}' not found. Please create it first: gcloud pubsub topics create {TOPIC_NAME}")
            st.error(f"Error: {topic_error}")
            return None, None
        
        return publisher, topic_path
    except Exception as e:
        st.error(f"❌ Failed to initialize Pub/Sub client: {e}")
        st.error("Please check your authentication setup:")
        return None, None

# Helper functions
def publish_to_pubsub(data: dict) -> str:
    """Publish data to Pub/Sub topic."""
    publisher, topic_path = init_pubsub_client()
    if not publisher:
        return None
        
    try:
        message_json = json.dumps(data)
        message_bytes = message_json.encode('utf-8')
        
        future = publisher.publish(topic_path, message_bytes)
        message_id = future.result()
        
        return message_id
    except Exception as e:
        st.error(f"Error publishing to Pub/Sub: {e}")
        return None

def create_base_event(event_type: str, user_id: str, message: str, value: float = 0.0) -> dict:
    """Create base event structure."""
    return {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "message": message,
        "user_id": user_id,
        "event_type": event_type,
        "value": value,
        "source": "streamlit_app"
    }

# Initialize session state
if 'published_events' not in st.session_state:
    st.session_state.published_events = []

if 'auto_publish' not in st.session_state:
    st.session_state.auto_publish = False

# Main Streamlit App
def main():
    st.set_page_config(
        page_title="Streaming Data Publisher",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("🚀 Streaming Data Publisher")
    st.markdown("### Publish events to Google Cloud Pub/Sub for your streaming pipeline")
    
    # Sidebar configuration
    st.sidebar.header("Configuration")
    st.sidebar.info(f"**Project ID:** {PROJECT_ID}")
    st.sidebar.info(f"**Topic:** {TOPIC_NAME}")
    
    # Test connection
    if st.sidebar.button("Test Connection"):
        publisher, topic_path = init_pubsub_client()
        if publisher:
            st.sidebar.success("✅ Connected to Pub/Sub!")
        else:
            st.sidebar.error("❌ Failed to connect to Pub/Sub")
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📝 Manual Events", 
        "🔄 Auto Publishing", 
        "📊 Analytics", 
        "📈 Monitoring", 
        "⚙️ Settings"
    ])
    
    with tab1:
        manual_events_tab()
    
    with tab2:
        auto_publishing_tab()
    
    with tab3:
        analytics_tab()
    
    with tab4:
        monitoring_tab()
    
    with tab5:
        settings_tab()

def manual_events_tab():
    """Tab for manually creating and publishing events."""
    st.header("📝 Manual Event Publishing")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Create Event")
        
        # Event form
        with st.form("event_form"):
            event_type = st.selectbox(
                "Event Type",
                ["login", "logout", "purchase", "page_view", "search", "add_to_cart", "custom"]
            )
            
            if event_type == "custom":
                event_type = st.text_input("Custom Event Type")
            
            user_id = st.text_input("User ID", value=f"user_{random.randint(1, 100)}")
            message = st.text_area("Message", value="User event from Streamlit")
            value = st.number_input("Value", min_value=0.0, value=1.0, step=0.1)
            
            # Additional fields
            st.subheader("Additional Fields (Optional)")
            additional_fields = {}
            
            if event_type == "purchase":
                product_id = st.text_input("Product ID", value=f"prod_{random.randint(100, 999)}")
                currency = st.selectbox("Currency", ["USD", "EUR", "GBP"])
                additional_fields.update({"product_id": product_id, "currency": currency})
            
            elif event_type == "page_view":
                page = st.text_input("Page", value="/home")
                referrer = st.text_input("Referrer", value="")
                additional_fields.update({"page": page, "referrer": referrer})
            
            submitted = st.form_submit_button("Publish Event")
            
            if submitted:
                event_data = create_base_event(event_type, user_id, message, value)
                event_data.update(additional_fields)
                
                message_id = publish_to_pubsub(event_data)
                
                if message_id:
                    st.success(f"✅ Event published! Message ID: {message_id}")
                    st.session_state.published_events.append({
                        "timestamp": datetime.now(),
                        "event_type": event_type,
                        "user_id": user_id,
                        "message_id": message_id,
                        "value": value
                    })
                else:
                    st.error("❌ Failed to publish event")
    
    with col2:
        st.subheader("Quick Actions")
        
        # Quick publish buttons
        if st.button("🔐 Quick Login Event"):
            quick_publish_event("login", "Quick login event")
        
        if st.button("💰 Quick Purchase Event"):
            quick_publish_event("purchase", "Quick purchase event", random.uniform(10, 100))
        
        if st.button("👁️ Quick Page View Event"):
            quick_publish_event("page_view", "Quick page view event")
        
        # Bulk publish
        st.subheader("Bulk Publish")
        num_events = st.number_input("Number of Events", min_value=1, max_value=100, value=10)
        
        if st.button("📦 Bulk Publish"):
            bulk_publish_events(num_events)

def auto_publishing_tab():
    """Tab for automatic event publishing."""
    st.header("🔄 Automatic Event Publishing")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Auto Publisher Settings")
        
        events_per_minute = st.slider("Events per minute", 1, 60, 10)
        duration_minutes = st.slider("Duration (minutes)", 1, 30, 5)
        
        event_types = st.multiselect(
            "Event Types to Generate",
            ["login", "logout", "purchase", "page_view", "search", "add_to_cart"],
            default=["login", "purchase", "page_view"]
        )
        
        col_start, col_stop = st.columns(2)
        
        with col_start:
            if st.button("▶️ Start Auto Publishing") and not st.session_state.auto_publish:
                st.session_state.auto_publish = True
                auto_publish_events(events_per_minute, duration_minutes, event_types)
        
        with col_stop:
            if st.button("⏹️ Stop Auto Publishing"):
                st.session_state.auto_publish = False
    
    with col2:
        st.subheader("Auto Publisher Status")
        
        if st.session_state.auto_publish:
            st.success("🟢 Auto publishing active")
        else:
            st.info("⚪ Auto publishing inactive")
        
        # Recent events
        st.subheader("Recent Events")
        if st.session_state.published_events:
            recent_events = st.session_state.published_events[-5:]
            for event in reversed(recent_events):
                st.text(f"{event['timestamp'].strftime('%H:%M:%S')} - {event['event_type']}")

def analytics_tab():
    """Tab for analytics and visualizations."""
    st.header("📊 Event Analytics")
    
    if not st.session_state.published_events:
        st.info("No events published yet. Go to Manual Events tab to start publishing.")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(st.session_state.published_events)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Event Types Distribution")
        event_counts = df['event_type'].value_counts()
        fig_pie = px.pie(values=event_counts.values, names=event_counts.index)
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.subheader("Events Over Time")
        df['hour'] = df['timestamp'].dt.hour
        hourly_counts = df.groupby('hour').size()
        fig_line = px.line(x=hourly_counts.index, y=hourly_counts.values)
        fig_line.update_layout(xaxis_title="Hour", yaxis_title="Event Count")
        st.plotly_chart(fig_line, use_container_width=True)
    
    # Event table
    st.subheader("Event History")
    st.dataframe(df[['timestamp', 'event_type', 'user_id', 'value']].sort_values('timestamp', ascending=False))

def monitoring_tab():
    """Tab for monitoring the pipeline."""
    st.header("📈 Pipeline Monitoring")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Real-time Metrics")
        
        # Metrics
        if st.session_state.published_events:
            total_events = len(st.session_state.published_events)
            recent_events = len([e for e in st.session_state.published_events 
                               if e['timestamp'] > datetime.now() - timedelta(minutes=5)])
            
            metric_col1, metric_col2, metric_col3 = st.columns(3)
            
            with metric_col1:
                st.metric("Total Events", total_events)
            
            with metric_col2:
                st.metric("Last 5 minutes", recent_events)
            
            with metric_col3:
                avg_value = sum(e['value'] for e in st.session_state.published_events) / total_events
                st.metric("Avg Value", f"{avg_value:.2f}")
    
    with col2:
        st.subheader("System Health")
        
        # Test connection
        if st.button("🔍 Check Pub/Sub Connection"):
            publisher, topic_path = init_pubsub_client()
            if publisher:
                st.success("✅ Pub/Sub: Connected")
            else:
                st.error("❌ Pub/Sub: Disconnected")
        
        # Clear events
        if st.button("🗑️ Clear Event History"):
            st.session_state.published_events = []
            st.success("Event history cleared")

def settings_tab():
    """Tab for application settings."""
    st.header("⚙️ Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Google Cloud Settings")
        st.info(f"**Project ID:** {PROJECT_ID}")
        st.info(f"**Topic Name:** {TOPIC_NAME}")
        
        st.subheader("Application Settings")
        
        # Auto-refresh
        auto_refresh = st.checkbox("Auto-refresh monitoring", value=False)
        if auto_refresh:
            st.rerun()
    
    with col2:
        st.subheader("Export Data")
        
        if st.session_state.published_events:
            df = pd.DataFrame(st.session_state.published_events)
            csv = df.to_csv(index=False)
            
            st.download_button(
                label="📥 Download Event History (CSV)",
                data=csv,
                file_name=f"event_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        st.subheader("Import Sample Data")
        if st.button("📄 Load Sample Events"):
            load_sample_events()

# Helper functions for tabs
def quick_publish_event(event_type: str, message: str, value: float = 1.0):
    """Quickly publish an event."""
    user_id = f"user_{random.randint(1, 100)}"
    event_data = create_base_event(event_type, user_id, message, value)
    
    message_id = publish_to_pubsub(event_data)
    
    if message_id:
        st.success(f"✅ {event_type.title()} event published!")
        st.session_state.published_events.append({
            "timestamp": datetime.now(),
            "event_type": event_type,
            "user_id": user_id,
            "message_id": message_id,
            "value": value
        })
    else:
        st.error("❌ Failed to publish event")

def bulk_publish_events(num_events: int):
    """Bulk publish multiple events."""
    event_types = ["login", "logout", "purchase", "page_view", "search", "add_to_cart"]
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    published_count = 0
    
    for i in range(num_events):
        event_type = random.choice(event_types)
        user_id = f"user_{random.randint(1, 50)}"
        message = f"Bulk event {i+1}"
        value = round(random.uniform(1.0, 100.0), 2)
        
        event_data = create_base_event(event_type, user_id, message, value)
        message_id = publish_to_pubsub(event_data)
        
        if message_id:
            published_count += 1
            st.session_state.published_events.append({
                "timestamp": datetime.now(),
                "event_type": event_type,
                "user_id": user_id,
                "message_id": message_id,
                "value": value
            })
        
        progress_bar.progress((i + 1) / num_events)
        status_text.text(f"Publishing event {i+1}/{num_events}")
        
        time.sleep(0.1)  # Small delay to avoid overwhelming
    
    st.success(f"✅ Bulk publish completed! {published_count}/{num_events} events published.")

def auto_publish_events(events_per_minute: int, duration_minutes: int, event_types: List[str]):
    """Auto publish events at specified rate."""
    if not event_types:
        st.error("Please select at least one event type")
        return
    
    total_events = events_per_minute * duration_minutes
    delay = 60.0 / events_per_minute
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i in range(total_events):
        if not st.session_state.auto_publish:
            break
        
        event_type = random.choice(event_types)
        user_id = f"user_{random.randint(1, 50)}"
        message = f"Auto-generated {event_type} event"
        value = round(random.uniform(1.0, 100.0), 2)
        
        event_data = create_base_event(event_type, user_id, message, value)
        message_id = publish_to_pubsub(event_data)
        
        if message_id:
            st.session_state.published_events.append({
                "timestamp": datetime.now(),
                "event_type": event_type,
                "user_id": user_id,
                "message_id": message_id,
                "value": value
            })
        
        progress_bar.progress((i + 1) / total_events)
        status_text.text(f"Auto-publishing: {i+1}/{total_events} events")
        
        time.sleep(delay)
    
    st.session_state.auto_publish = False
    st.success("✅ Auto-publishing completed!")

def load_sample_events():
    """Load sample events for testing."""
    sample_events = [
        {"event_type": "login", "user_id": "user_1", "value": 1.0},
        {"event_type": "purchase", "user_id": "user_2", "value": 29.99},
        {"event_type": "page_view", "user_id": "user_3", "value": 1.0},
        {"event_type": "search", "user_id": "user_4", "value": 1.0},
        {"event_type": "logout", "user_id": "user_1", "value": 1.0},
    ]
    
    for event in sample_events:
        st.session_state.published_events.append({
            "timestamp": datetime.now() - timedelta(minutes=random.randint(1, 60)),
            "event_type": event["event_type"],
            "user_id": event["user_id"],
            "message_id": f"sample_{uuid.uuid4().hex[:8]}",
            "value": event["value"]
        })
    
    st.success("✅ Sample events loaded!")

if __name__ == "__main__":
    main()
