import os
import time
import math
import random
import requests
from datetime import datetime, timezone

# 1. Configurable API URL via Environment Variable
API_URL = os.environ.get("INGESTION_API_URL", "http://localhost:8080/api/v1/logs")

# 2. Map realistic endpoints to specific service names
SERVICES = {
    "auth-service": ["/api/login", "/api/logout", "/api/refresh", "/api/verify"],
    "checkout-service": ["/api/cart/add", "/api/cart/view", "/api/checkout/process"],
    "payment-gateway": ["/api/payment/charge", "/api/payment/refund", "/api/payment/status"],
    "database-service": ["/db/query", "/db/insert", "/db/update"]
}

# Global Stateful Memory Leak Variable
memory_pressure = 0

def generate_timestamp():
    return datetime.now(timezone.utc).isoformat()

def generate_log_entry(service_name):
    endpoint = random.choice(SERVICES[service_name])
    timestamp = generate_timestamp()
    
    # Calculate Database Service Latency based on memory pressure
    db_base_latency = 20 + int(memory_pressure * 15)
    
    status_code = random.choice([200, 201])
    response_time = random.randint(20, 150)
    
    if service_name == "database-service":
        # Stateful Memory Leak affects database latency
        response_time = db_base_latency + random.randint(-10, 50)
        
    elif service_name == "checkout-service":
        # Cascading Failure: If DB latency > 800ms, checkout starts dropping requests
        if db_base_latency > 800:
            # 80% chance to fail
            if random.random() < 0.8:
                status_code = random.choice([500, 503])
                response_time = random.randint(500, 2000)

    return {
        "timestamp": timestamp,
        "serviceName": service_name,
        "endpoint": endpoint,
        "statusCode": status_code,
        "responseTimeMs": response_time
    }

def main():
    global memory_pressure
    print(f"Starting Advanced Data Generator... Target API: {API_URL}")
    print("Press Ctrl+C to stop.")
    
    # 3. Graceful shutdown using try/except KeyboardInterrupt
    try:
        while True:
            # Sleep 1 to 3 seconds
            time.sleep(random.uniform(1, 3))
            
            # Smoother Sine Wave Traffic: Divisor of 60 simulates longer traffic curves
            sine_val = (math.sin(time.time() / 60.0) + 1) / 2.0
            batch_size = int(10 + (140 * sine_val))
            
            # Increment Memory Pressure
            memory_pressure += random.uniform(0.5, 1.5)
            
            # Server Restart condition
            db_base_latency = 20 + int(memory_pressure * 15)
            if db_base_latency > 2000:
                print("\n🔥 CRITICAL: Database Service Latency exceeded 2000ms! 🔥")
                print("🔄 Automated Server Restart Initiated. Resetting Memory Pressure to 0. 🔄\n")
                memory_pressure = 0
            
            logs = []
            for _ in range(batch_size):
                svc = random.choice(list(SERVICES.keys()))
                logs.append(generate_log_entry(svc))
            
            payload = {"logs": logs}
            
            try:
                response = requests.post(API_URL, json=payload, timeout=5)
                # Print Summary
                print(f"Sent {batch_size} logs | Sine Traffic: {sine_val:.2f} | DB Pressure: {memory_pressure:.1f} | Status: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to send batch: {e}")
                
    except KeyboardInterrupt:
        print("\nShutting down Data Generator gracefully...")

if __name__ == "__main__":
    main()
