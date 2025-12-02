import requests
import time
import random

def fake_window(num_msgs=256):
    messages = []
    now_ns = time.time_ns()
    for i in range(num_msgs):
        messages.append({
            "timestamp_ns": now_ns + i * 1000,
            "exchange_name": "EX_A",
            "stream_id": "test_stream",
            "msg_type": random.choice(["NEW", "ACK", "NACK", "HEARTBEAT"]),
            "seq_num": i + 1,
            "payload_size": random.randint(100, 1000),
        })
    return messages

if __name__ == "__main__":
    url = "http://localhost:8000/predict_window"
    window = fake_window()
    payload = {"messages": window}

    resp = requests.post(url, json=payload)
    print("Status:", resp.status_code)
    print("Response:", resp.json())
