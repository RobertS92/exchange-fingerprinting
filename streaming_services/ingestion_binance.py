# streaming_services/ingestion_binance.py
"""
Binance WebSocket ingestion service.
Connects to Binance order book stream and publishes raw messages to Kafka.
"""
import asyncio
import json
import time
from typing import Dict, Any
import websockets
from confluent_kafka import Producer
from src.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BinanceIngestion:
    def __init__(self, symbol: str = "btcusdt", kafka_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        self.symbol = symbol.lower()
        self.kafka_servers = kafka_servers
        self.producer = None
        self.msg_count = 0
        self.seq_num = 0
        
    def _init_producer(self):
        """Initialize Kafka producer."""
        conf = {
            'bootstrap.servers': self.kafka_servers,
            'client.id': f'binance_ingestion_{self.symbol}'
        }
        self.producer = Producer(conf)
        logger.info(f"Kafka producer initialized: {self.kafka_servers}")
        
    def _delivery_report(self, err, msg):
        """Kafka delivery callback."""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            self.msg_count += 1
            if self.msg_count % 100 == 0:
                logger.info(f'Delivered {self.msg_count} messages to {msg.topic()}')
    
    def _normalize_message(self, raw_msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize Binance WebSocket message to our internal format.
        """
        timestamp_ns = time.time_ns()
        
        # Determine message type based on Binance message structure
        msg_type = "UNKNOWN"
        if "e" in raw_msg:  # Event type
            event = raw_msg["e"]
            if event == "depthUpdate":
                msg_type = "UPDATE"
            elif event == "trade":
                msg_type = "TRADE"
        
        # For demo purposes, simulate some message types
        # In production, you'd parse actual Binance message types
        normalized = {
            "timestamp_ns": timestamp_ns,
            "exchange_name": "binance",
            "stream_id": f"{self.symbol}@depth",
            "msg_type": msg_type,
            "seq_num": self.seq_num,
            "payload_size": len(json.dumps(raw_msg)),
            "raw_data": raw_msg  # Keep original for debugging
        }
        
        self.seq_num += 1
        return normalized
    
    async def connect_and_stream(self):
        """Connect to Binance WebSocket and stream to Kafka."""
        self._init_producer()
        
        # Binance WebSocket URL for order book depth
        ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth@100ms"
        
        logger.info(f"Connecting to Binance WebSocket: {ws_url}")
        
        retry_count = 0
        max_retries = 5
        
        while retry_count < max_retries:
            try:
                async with websockets.connect(ws_url) as websocket:
                    logger.info(f"✅ Connected to Binance {self.symbol} stream")
                    retry_count = 0  # Reset on successful connection
                    
                    async for message in websocket:
                        try:
                            # Parse the WebSocket message
                            raw_msg = json.loads(message)
                            
                            # Normalize to our format
                            normalized_msg = self._normalize_message(raw_msg)
                            
                            # Publish to Kafka
                            self.producer.produce(
                                RAW_TOPIC,
                                key=normalized_msg["stream_id"].encode('utf-8'),
                                value=json.dumps(normalized_msg).encode('utf-8'),
                                callback=self._delivery_report
                            )
                            
                            # Poll to trigger delivery callbacks
                            self.producer.poll(0)
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse message: {e}")
                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            
            except websockets.exceptions.WebSocketException as e:
                retry_count += 1
                logger.error(f"WebSocket error (retry {retry_count}/{max_retries}): {e}")
                await asyncio.sleep(5 * retry_count)  # Exponential backoff
                
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                break
        
        logger.error("Max retries reached. Exiting.")
        
    def close(self):
        """Cleanup resources."""
        if self.producer:
            logger.info("Flushing remaining messages...")
            self.producer.flush()
            logger.info(f"✅ Ingestion complete. Total messages: {self.msg_count}")


async def main():
    """Main entry point."""
    ingestion = BinanceIngestion(symbol="btcusdt")
    
    try:
        await ingestion.connect_and_stream()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        ingestion.close()


if __name__ == "__main__":
    asyncio.run(main())

