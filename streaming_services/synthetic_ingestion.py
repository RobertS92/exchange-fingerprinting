# streaming_services/synthetic_ingestion.py
"""
Synthetic data generator for testing the pipeline.
Simulates exchange message streams and publishes to Kafka.
"""
import json
import time
import random
from typing import Dict, List
from confluent_kafka import Producer
from src.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SyntheticIngestion:
    def __init__(self):
        self.producer = None
        self.msg_count = 0
        self.exchanges = {
            "binance": {
                "base_rate_hz": 100,
                "ack_rate": 0.8,
                "nack_rate": 0.1,
                "heartbeat_rate_hz": 2,
            },
            "coinbase": {
                "base_rate_hz": 80,
                "ack_rate": 0.7,
                "nack_rate": 0.15,
                "heartbeat_rate_hz": 1,
            },
            "kraken": {
                "base_rate_hz": 60,
                "ack_rate": 0.75,
                "nack_rate": 0.12,
                "heartbeat_rate_hz": 1.5,
            }
        }
        
    def _init_producer(self):
        """Initialize Kafka producer."""
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'synthetic_ingestion'
        }
        self.producer = Producer(conf)
        logger.info(f"Kafka producer initialized: {KAFKA_BOOTSTRAP_SERVERS}")
        
    def _delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            self.msg_count += 1
            if self.msg_count % 1000 == 0:
                logger.info(f'✅ Delivered {self.msg_count} messages')
    
    def _generate_message(self, exchange_name: str, stream_id: str, seq_num: int) -> Dict:
        """Generate a synthetic message for an exchange."""
        params = self.exchanges[exchange_name]
        
        # Determine message type
        rand = random.random()
        if rand < 0.05:  # 5% heartbeats
            msg_type = "HEARTBEAT"
        elif rand < 0.50:  # 45% NEW orders
            msg_type = "NEW"
        elif rand < 0.85:  # 35% ACKs
            msg_type = "ACK"
        elif rand < 0.95:  # 10% NACKs
            msg_type = "NACK"
        else:  # 5% other
            msg_type = "UPDATE"
        
        # Add some exchange-specific patterns
        if exchange_name == "binance":
            # Binance tends to have faster acks
            if msg_type == "ACK" and random.random() < 0.3:
                msg_type = "ACK"  # More consistent
        elif exchange_name == "coinbase":
            # Coinbase has more NACKs during high load
            if msg_type == "NEW" and random.random() < 0.2:
                msg_type = "NACK"
        elif exchange_name == "kraken":
            # Kraken has more regular heartbeats
            if random.random() < 0.08:
                msg_type = "HEARTBEAT"
        
        message = {
            "timestamp_ns": time.time_ns(),
            "exchange_name": exchange_name,
            "stream_id": stream_id,
            "msg_type": msg_type,
            "seq_num": seq_num,
            "payload_size": random.randint(100, 500)
        }
        
        return message
    
    def run(self, duration_sec: int = 300, rate_multiplier: float = 0.1):
        """
        Run the synthetic data generator.
        
        Args:
            duration_sec: How long to run (seconds)
            rate_multiplier: Scale factor for message rates (0.1 = 10% of normal rate)
        """
        self._init_producer()
        
        logger.info(f"🚀 Starting synthetic ingestion for {duration_sec}s at {rate_multiplier}x rate")
        
        start_time = time.time()
        seq_nums = {exchange: 0 for exchange in self.exchanges}
        
        try:
            while time.time() - start_time < duration_sec:
                # Generate messages for each exchange
                for exchange_name in self.exchanges:
                    params = self.exchanges[exchange_name]
                    base_rate = params["base_rate_hz"] * rate_multiplier
                    
                    # Determine how many messages to send this iteration
                    num_msgs = max(1, int(base_rate * 0.1))  # 100ms window
                    
                    for _ in range(num_msgs):
                        stream_id = f"{exchange_name}@btcusdt"
                        message = self._generate_message(
                            exchange_name,
                            stream_id,
                            seq_nums[exchange_name]
                        )
                        seq_nums[exchange_name] += 1
                        
                        # Publish to Kafka
                        self.producer.produce(
                            RAW_TOPIC,
                            key=stream_id.encode('utf-8'),
                            value=json.dumps(message).encode('utf-8'),
                            callback=self._delivery_report
                        )
                
                # Poll and sleep
                self.producer.poll(0)
                time.sleep(0.1)  # 100ms intervals
                
        except KeyboardInterrupt:
            logger.info("Received shutdown signal")
        finally:
            self.close()
    
    def close(self):
        """Cleanup resources."""
        if self.producer:
            logger.info("Flushing remaining messages...")
            self.producer.flush()
            logger.info(f"✅ Ingestion complete. Total messages: {self.msg_count}")


def main():
    """Main entry point."""
    generator = SyntheticIngestion()
    generator.run(duration_sec=600, rate_multiplier=0.5)  # Run for 10 minutes at 50% rate


if __name__ == "__main__":
    main()

