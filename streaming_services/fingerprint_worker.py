# streaming_services/fingerprint_worker.py
"""
Fingerprint worker service.
Consumes raw messages from Kafka, builds windows, extracts features,
calls model API for classification, and publishes to classified topic.
"""
import json
import os
import time
from collections import deque
from typing import Dict, List, Any
import pandas as pd
import requests
from confluent_kafka import Consumer, Producer, KafkaException
from src.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC, CLASSIFIED_TOPIC
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
WINDOW_SIZE = 50  # Number of messages per window
STRIDE = 25  # Window stride (overlap = WINDOW_SIZE - STRIDE)
BUFFER_MAX_SIZE = 500  # Max messages to keep in buffer per stream
MODEL_API_URL = os.getenv("MODEL_URL", "http://model_api:8000/predict_window")


class FingerprintWorker:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.stream_buffers: Dict[str, deque] = {}  # stream_id -> deque of messages
        self.windows_processed = 0
        
    def _init_consumer(self):
        """Initialize Kafka consumer."""
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'fingerprint_worker_group',
            'auto.offset.reset': 'latest',  # Start from latest messages
            'enable.auto.commit': True,
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([RAW_TOPIC])
        logger.info(f"Kafka consumer initialized, subscribed to {RAW_TOPIC}")
        
    def _init_producer(self):
        """Initialize Kafka producer."""
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'fingerprint_worker'
        }
        self.producer = Producer(conf)
        logger.info("Kafka producer initialized")
        
    def _delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            self.windows_processed += 1
            if self.windows_processed % 10 == 0:
                logger.info(f'Processed {self.windows_processed} windows')
    
    def _add_to_buffer(self, message: Dict[str, Any]):
        """Add message to the appropriate stream buffer."""
        stream_id = message.get("stream_id", "unknown")
        
        if stream_id not in self.stream_buffers:
            self.stream_buffers[stream_id] = deque(maxlen=BUFFER_MAX_SIZE)
            logger.info(f"Created new buffer for stream: {stream_id}")
        
        self.stream_buffers[stream_id].append(message)
    
    def _build_windows(self, stream_id: str) -> List[List[Dict]]:
        """Build sliding windows from buffer."""
        buffer = self.stream_buffers.get(stream_id, deque())
        
        if len(buffer) < WINDOW_SIZE:
            return []
        
        windows = []
        msgs = list(buffer)
        
        # Create overlapping windows
        i = 0
        while i + WINDOW_SIZE <= len(msgs):
            windows.append(msgs[i:i+WINDOW_SIZE])
            i += STRIDE
        
        return windows
    
    def _classify_window(self, window_messages: List[Dict]) -> Dict[str, Any]:
        """
        Send window to model API for classification.
        Returns classification result or None if failed.
        """
        try:
            # Prepare payload for model API
            payload = {
                "messages": [
                    {
                        "timestamp_ns": msg["timestamp_ns"],
                        "exchange_name": msg.get("exchange_name", "unknown"),
                        "stream_id": msg.get("stream_id", "unknown"),
                        "msg_type": msg.get("msg_type", "UNKNOWN"),
                        "seq_num": msg.get("seq_num"),
                        "payload_size": msg.get("payload_size", 0)
                    }
                    for msg in window_messages
                ]
            }
            
            # Call model API
            response = requests.post(
                MODEL_API_URL,
                json=payload,
                timeout=5
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Model API error: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to call model API: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in classification: {e}")
            return None
    
    def _process_stream(self, stream_id: str):
        """Process all complete windows for a stream."""
        windows = self._build_windows(stream_id)
        
        for window_msgs in windows:
            # Get window metadata
            first_msg = window_msgs[0]
            last_msg = window_msgs[-1]
            
            # Classify the window
            prediction = self._classify_window(window_msgs)
            
            if prediction:
                # Create classified window message
                classified_msg = {
                    "timestamp_start_ns": first_msg["timestamp_ns"],
                    "timestamp_end_ns": last_msg["timestamp_ns"],
                    "exchange_name": first_msg.get("exchange_name", "unknown"),
                    "stream_id": stream_id,
                    "predicted_exchange": prediction["predicted_exchange"],
                    "probabilities": prediction["probabilities"],
                    "confidence": max(prediction["probabilities"].values()),
                    "num_messages": len(window_msgs),
                    "processed_at": time.time_ns()
                }
                
                # Publish to classified topic
                self.producer.produce(
                    CLASSIFIED_TOPIC,
                    key=stream_id.encode('utf-8'),
                    value=json.dumps(classified_msg).encode('utf-8'),
                    callback=self._delivery_report
                )
                self.producer.poll(0)
    
    def run(self):
        """Main processing loop."""
        self._init_consumer()
        self._init_producer()
        
        logger.info("🚀 Fingerprint worker started")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    raise KafkaException(msg.error())
                
                try:
                    # Parse message
                    message = json.loads(msg.value().decode('utf-8'))
                    
                    # Add to buffer
                    self._add_to_buffer(message)
                    
                    # Process windows for this stream
                    stream_id = message.get("stream_id", "unknown")
                    self._process_stream(stream_id)
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.close()
    
    def close(self):
        """Cleanup resources."""
        if self.producer:
            logger.info("Flushing producer...")
            self.producer.flush()
        if self.consumer:
            logger.info("Closing consumer...")
            self.consumer.close()
        logger.info(f"✅ Worker shutdown complete. Windows processed: {self.windows_processed}")


def main():
    """Main entry point."""
    worker = FingerprintWorker()
    worker.run()


if __name__ == "__main__":
    main()

