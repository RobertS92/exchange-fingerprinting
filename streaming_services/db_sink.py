# streaming_services/db_sink.py
"""
Database sink service.
Consumes classified windows from Kafka and saves them to Postgres.
"""
import json
from typing import Dict, Any
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import create_engine, text
from src.config import KAFKA_BOOTSTRAP_SERVERS, CLASSIFIED_TOPIC, DB_URL
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DatabaseSink:
    def __init__(self):
        self.consumer = None
        self.engine = None
        self.records_saved = 0
        
    def _init_consumer(self):
        """Initialize Kafka consumer."""
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'db_sink_group',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([CLASSIFIED_TOPIC])
        logger.info(f"Kafka consumer initialized, subscribed to {CLASSIFIED_TOPIC}")
        
    def _init_database(self):
        """Initialize database connection."""
        self.engine = create_engine(DB_URL)
        logger.info("Database connection initialized")
        
        # Verify table exists
        with self.engine.begin() as conn:
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'classified_windows'
                );
            """))
            exists = result.scalar()
            if not exists:
                logger.error("Table 'classified_windows' does not exist!")
                raise Exception("Database schema not initialized. Run init_db.py first.")
            else:
                logger.info("✅ Database table verified")
    
    def _save_to_database(self, classified_window: Dict[str, Any]):
        """Save a classified window to the database."""
        try:
            insert_sql = text("""
                INSERT INTO classified_windows (
                    timestamp_start_ns,
                    timestamp_end_ns,
                    exchange_name,
                    stream_id,
                    predicted_exchange,
                    confidence,
                    num_messages,
                    probabilities
                ) VALUES (
                    :timestamp_start_ns,
                    :timestamp_end_ns,
                    :exchange_name,
                    :stream_id,
                    :predicted_exchange,
                    :confidence,
                    :num_messages,
                    CAST(:probabilities AS jsonb)
                )
            """)
            
            with self.engine.begin() as conn:
                conn.execute(insert_sql, {
                    'timestamp_start_ns': classified_window['timestamp_start_ns'],
                    'timestamp_end_ns': classified_window['timestamp_end_ns'],
                    'exchange_name': classified_window.get('exchange_name', 'unknown'),
                    'stream_id': classified_window['stream_id'],
                    'predicted_exchange': classified_window['predicted_exchange'],
                    'confidence': classified_window.get('confidence', 0.0),
                    'num_messages': classified_window.get('num_messages', 0),
                    'probabilities': json.dumps(classified_window.get('probabilities', {}))
                })
            
            self.records_saved += 1
            
            if self.records_saved % 10 == 0:
                logger.info(f"💾 Saved {self.records_saved} classified windows to database")
                
        except Exception as e:
            logger.error(f"Failed to save to database: {e}")
            logger.error(f"Problematic data: {classified_window}")
    
    def run(self):
        """Main processing loop."""
        self._init_consumer()
        self._init_database()
        
        logger.info("🚀 Database sink started")
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    raise KafkaException(msg.error())
                
                try:
                    # Parse classified window message
                    classified_window = json.loads(msg.value().decode('utf-8'))
                    
                    # Save to database
                    self._save_to_database(classified_window)
                    
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
        if self.consumer:
            logger.info("Closing consumer...")
            self.consumer.close()
        if self.engine:
            logger.info("Closing database connection...")
            self.engine.dispose()
        logger.info(f"✅ DB sink shutdown complete. Records saved: {self.records_saved}")


def main():
    """Main entry point."""
    sink = DatabaseSink()
    sink.run()


if __name__ == "__main__":
    main()

