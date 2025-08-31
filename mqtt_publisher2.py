import csv
import time
import ssl
import json
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# AWS IoT Core connection settings
MQTT_ENDPOINT = "ackvac20gj6g5-ats.iot.us-east-1.amazonaws.com"
PORT = 8883
CA_CERT = "AmazonRootCA1.pem"
DEVICE_CERT = "cert.pem.crt"
PRIVATE_KEY = "private.pem.key"
TOPIC_PREFIX = "/vehicle/Vehicle01"

# MQTT client setup
client = mqtt.Client(protocol=mqtt.MQTTv311)
client.tls_set(ca_certs=CA_CERT,
               certfile=DEVICE_CERT,
               keyfile=PRIVATE_KEY,
               tls_version=ssl.PROTOCOL_TLSv1_2)

print("Connecting to MQTT broker...")
client.connect(MQTT_ENDPOINT, PORT, keepalive=60)
client.loop_start()

# Simulated start time
simulated_time = datetime.now()

# === Load CSV and clean ===
with open('logistics_dataset.csv', 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    rows = list(reader)

if not rows:
    print("No data found in CSV.")
    exit()

# Clean keys/values
cleaned_rows = []
for row in rows:
    if row:
        cleaned_rows.append({k.strip(): v.strip() for k, v in row.items() if k and v})

# === Send Fixed Features from the First Row ===
fixed = cleaned_rows[0]
fixed_payloads = {
    f"{TOPIC_PREFIX}/lead_time_days": float(fixed["lead_time_days"]),
    f"{TOPIC_PREFIX}/supplier_reliability_score": float(fixed["supplier_reliability_score"]),
    f"{TOPIC_PREFIX}/port_congestion_level": float(fixed["port_congestion_level"]),
    f"{TOPIC_PREFIX}/weather_condition_severity": float(fixed["weather_condition_severity"]),
    f"{TOPIC_PREFIX}/route_risk_level": float(fixed["route_risk_level"]),
    f"{TOPIC_PREFIX}/handling_equipment_availability": float(fixed["handling_equipment_availability"]),
    f"{TOPIC_PREFIX}/historical_demand": float(fixed["historical_demand"]),
    f"{TOPIC_PREFIX}/driver_behavior_score": float(fixed["driver_behavior_score"]),
    f"{TOPIC_PREFIX}/warehouse_inventory_level": float(fixed["warehouse_inventory_level"]),
    f"{TOPIC_PREFIX}/loading_unloading_time": float(fixed["loading_unloading_time"])
}

print("\nSending fixed features (once at start):")
for topic, value in fixed_payloads.items():
    message = json.dumps({"value": value})
    client.publish(topic, message)
    print(f"Published to {topic}: {message}")

# === Real-time and Periodic loop (infinite) ===
row_count = 0
realtime_history = []
print("\nStarting real-time + periodic data stream (infinite loop):")

try:
    while True:
        # Cycle through cleaned rows (excluding first fixed row)
        row = cleaned_rows[(row_count % (len(cleaned_rows) - 1)) + 1]

        trip_duration_seconds = (row_count + 1) * 10

        # Real-time features
        realtime_payloads = {
            f"{TOPIC_PREFIX}/vehicle_gps_latitude": float(row["vehicle_gps_latitude"]),
            f"{TOPIC_PREFIX}/vehicle_gps_longitude": float(row["vehicle_gps_longitude"]),
            f"{TOPIC_PREFIX}/fuel_consumption_rate": float(row["fuel_consumption_rate"]),
            f"{TOPIC_PREFIX}/traffic_congestion_level": float(row["traffic_congestion_level"]),
            f"{TOPIC_PREFIX}/iot_temperature": float(row["iot_temperature"]),
            f"{TOPIC_PREFIX}/cargo_condition_status": float(row["cargo_condition_status"]),
            f"{TOPIC_PREFIX}/trip_duration": float(trip_duration_seconds)
        }

        print(f"\nSimulated time: {simulated_time.strftime('%Y-%m-%d %H:%M:%S')}")
        for topic, value in realtime_payloads.items():
            message = json.dumps({"value": value})
            client.publish(topic, message)
            print(f"Published to {topic}: {message}")

        # Store for averaging
        realtime_row_clean = {
            "vehicle_gps_latitude": float(row["vehicle_gps_latitude"]),
            "vehicle_gps_longitude": float(row["vehicle_gps_longitude"]),
            "fuel_consumption_rate": float(row["fuel_consumption_rate"]),
            "traffic_congestion_level": float(row["traffic_congestion_level"]),
            "iot_temperature": float(row["iot_temperature"]),
            "cargo_condition_status": float(row["cargo_condition_status"]),
            "trip_duration": float(trip_duration_seconds)
        }
        realtime_history.append(realtime_row_clean)

        # Every 6 rows → send periodic + trigger
        if (row_count + 1) % 6 == 0:
            periodic_payloads = {
                f"{TOPIC_PREFIX}/fatigue_monitoring_score": float(row["fatigue_monitoring_score"]),
                f"{TOPIC_PREFIX}/disruption_likelihood_score": float(row["disruption_likelihood_score"]),
                f"{TOPIC_PREFIX}/eta_variation_hours": float(row["eta_variation_hours"]),
                f"{TOPIC_PREFIX}/order_fulfillment_status": float(row["order_fulfillment_status"]),
                f"{TOPIC_PREFIX}/shipping_costs": float(row["shipping_costs"]),
                f"{TOPIC_PREFIX}/customs_clearance_time": float(row["customs_clearance_time"]),
                f"{TOPIC_PREFIX}/delay_probability": float(row["delay_probability"]),
                f"{TOPIC_PREFIX}/delivery_time_deviation": float(row["delivery_time_deviation"])
            }

            print("\nSending periodic features:")
            for topic, value in periodic_payloads.items():
                message = json.dumps({"value": value})
                client.publish(topic, message)
                print(f"Published to {topic}: {message}")

            # Build trigger payload
            trigger_payload = {
                "fixed": {k.replace(f"{TOPIC_PREFIX}/", ""): v for k, v in fixed_payloads.items()},
                "periodic": {k.replace(f"{TOPIC_PREFIX}/", ""): v for k, v in periodic_payloads.items()},
                "realtime": realtime_history
            }

            # Send trigger to prediction topic
            client.publish(f"{TOPIC_PREFIX}/trigger_prediction", json.dumps(trigger_payload))
            print(f"\n📣 Published trigger to {TOPIC_PREFIX}/trigger_prediction")

            realtime_history = []

        time.sleep(10)
        simulated_time += timedelta(seconds=10)
        row_count += 1

except KeyboardInterrupt:
    print("\nStreaming stopped by user (Ctrl+C).")

finally:
    client.loop_stop()
    client.disconnect()
