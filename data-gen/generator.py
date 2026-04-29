import os
import time
import random
import json
import requests
from datetime import datetime, timezone

# 1. Configurable API URL via Environment Variable
API_URL = os.environ.get("INGESTION_API_URL", "http://localhost:8080/api/v1/logs")

# 2. Map realistic endpoints to specific service names
SERVICES = {
    "auth-service": ["/api/login", "/api/logout", "/api/refresh", "/api/verify"],
    "checkout-service": ["/api/cart/add", "/api/cart/view", "/api/checkout/process"],
    "payment-gateway": ["/api/payment/charge", "/api/payment/refund", "/api/payment/status"]
}

def generate_timestamp():
    return datetime.now(timezone.utc).isoformat()

def generate_log_entry(chaos_type=None):
    service_name = random.choice(list(SERVICES.keys()))
    endpoint = random.choice(SERVICES[service_name])
    
    timestamp = generate_timestamp()
    
    if chaos_type == "error_storm":
        status_code = random.choice([500, 503])
        response_time = random.randint(20, 150)
    elif chaos_type == "latency_spike":
        status_code = random.choice([200, 201])
        response_time = random.randint(3000, 8000)
    else:
        # Normal Traffic
        status_code = random.choice([200, 201])
        response_time = random.randint(20, 150)
        
    return {
        "timestamp": timestamp,
        "serviceName": service_name,
        "endpoint": endpoint,
        "statusCode": status_code,
        "responseTimeMs": response_time
    }

def main():
    print(f"Starting Data Generator... Target API: {API_URL}")
    print("Press Ctrl+C to stop.")
    
    # 3. Graceful shutdown using try/except KeyboardInterrupt
    try:
        while True:
            # Sleep 1 to 3 seconds
            time.sleep(random.uniform(1, 3))
            
            batch_size = random.randint(10, 50)
            
            # Determine if this batch is Chaos
            is_chaos = random.random() < 0.4
            chaos_type = None
            if is_chaos:
                chaos_type = random.choice(["error_storm", "latency_spike"])
                
            logs = [generate_log_entry(chaos_type) for _ in range(batch_size)]
            
            payload = {"logs": logs}
            
            try:
                response = requests.post(API_URL, json=payload, timeout=5)
                
                # Print Summary
                if not is_chaos:
                    print(f"Sent {batch_size} logs (Normal) - Status: {response.status_code}")
                else:
                    if chaos_type == "error_storm":
                        print(f"Sent {batch_size} logs (CHAOS: Error Storm) - Status: {response.status_code}")
                    else:
                        print(f"Sent {batch_size} logs (CHAOS: Latency Spike) - Status: {response.status_code}")
            except requests.exceptions.RequestException as e:
                print(f"Failed to send batch: {e}")
                
    except KeyboardInterrupt:
        print("\nShutting down Data Generator gracefully...")

if __name__ == "__main__":
    main()
