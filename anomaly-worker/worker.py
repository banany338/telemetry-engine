import os
import time
import uuid
import pandas as pd
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

DB_USER = os.environ.get("DB_USER", "admin")
DB_PASS = os.environ.get("DB_PASS", "password")
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "telemetry")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Instantiate SQLAlchemy engine outside the loop to reuse connection pool
engine = create_engine(DATABASE_URL)

def record_anomaly(timestamp, service_name, anomaly_type, description):
    print(f"\n🚨 [ANOMALY DETECTED] 🚨")
    print(f"Service: {service_name} | Type: {anomaly_type} | Desc: {description}")
    
    insert_query = text("""
        INSERT INTO anomalies (id, timestamp, service_name, anomaly_type, description)
        VALUES (:id, :timestamp, :service_name, :anomaly_type, :description)
    """)
    
    with engine.begin() as conn:
        conn.execute(insert_query, {
            "id": str(uuid.uuid4()),
            "timestamp": timestamp,
            "service_name": service_name,
            "anomaly_type": anomaly_type,
            "description": description
        })

def analyze_window():
    # 1. 60-second time window strictly timezone-aware (UTC)
    now_utc = datetime.now(timezone.utc)
    sixty_seconds_ago = now_utc - timedelta(seconds=60)
    
    query = f"""
        SELECT * FROM raw_logs 
        WHERE timestamp >= '{sixty_seconds_ago.isoformat()}'
    """
    
    # Load into Pandas directly from SQLAlchemy engine
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print(f"[{now_utc.isoformat()}] No new data in the last 60s.")
        return
    
    print(f"[{now_utc.isoformat()}] Processing batch of {len(df)} logs...")
    
    # Group by service name
    grouped = df.groupby('service_name')
    
    for service_name, group in grouped:
        total_requests = len(group)
        
        # Calculate Error Rate
        non_success_count = len(group[~group['status_code'].isin([200, 201])])
        error_rate = non_success_count / total_requests
        
        if error_rate > 0.05:
            description = f"Error rate reached {error_rate * 100:.2f}% ({non_success_count}/{total_requests} requests failed)"
            record_anomaly(now_utc, service_name, "error_storm", description)
            
        # Calculate Latency Check
        avg_latency = group['response_time_ms'].mean()
        
        if avg_latency > 1000:
            description = f"Average latency spiked to {avg_latency:.2f}ms"
            record_anomaly(now_utc, service_name, "latency_spike", description)


def main():
    print(f"Starting Anomaly Detection Worker... Connected to {DB_HOST}:{DB_PORT}")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            try:
                analyze_window()
            except Exception as e:
                print(f"Error during analysis window: {e}")
                
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\nShutting down Anomaly Detection Worker gracefully...")
        engine.dispose()

if __name__ == "__main__":
    main()
