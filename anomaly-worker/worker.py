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

# In-memory history for Z-Score calculation (sliding window of max 10 points per service)
historical_stats = {}

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
    
    db_latency_high = False
    checkout_error_high = False
    
    for service_name, group in grouped:
        if service_name not in historical_stats:
            historical_stats[service_name] = []
            
        total_requests = len(group)
        
        # p95 Latency Calculation
        p95_latency = group['response_time_ms'].quantile(0.95)
        
        # Calculate Error Rate
        non_success_count = len(group[~group['status_code'].isin([200, 201])])
        error_rate = non_success_count / total_requests if total_requests > 0 else 0
        
        # Check for Cascading conditions
        if service_name == "database-service" and p95_latency > 800:
            db_latency_high = True
        if service_name == "checkout-service" and error_rate > 0.05:
            checkout_error_high = True
            
        history = historical_stats[service_name]
        
        # Cold Start Fix: Require at least 5 data points before calculating Z-Score
        if len(history) >= 5:
            hist_series = pd.Series(history)
            mean_latency = hist_series.mean()
            std_latency = hist_series.std()
            
            # Avoid ZeroDivisionError if std is 0
            if std_latency > 0:
                z_score = (p95_latency - mean_latency) / std_latency
                
                # Progressive Degradation Anomaly (Z-Score > 2)
                if z_score > 2:
                    desc = f"p95 latency ({p95_latency:.2f}ms) is {z_score:.2f} std devs above mean ({mean_latency:.2f}ms)"
                    record_anomaly(now_utc, service_name, "progressive_degradation", desc)
        
        # Append current p95 to history, keep sliding window of size 10
        history.append(p95_latency)
        if len(history) > 10:
            history.pop(0)
            
    # Cascade Detection Evaluation
    if db_latency_high and checkout_error_high:
        desc = "High database latency cascading into high checkout service error rates."
        record_anomaly(now_utc, "system-wide", "cascading_failure", desc)


def main():
    print(f"Starting Advanced Anomaly Worker... Connected to {DB_HOST}:{DB_PORT}")
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
