# init_db.py
"""Initialize database schema for exchange fingerprinting."""
import os
from sqlalchemy import create_engine, text
from src.config import DB_URL

def init_database():
    """Create the necessary database tables."""
    engine = create_engine(DB_URL)
    
    # Create classified_windows table
    create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS classified_windows (
            id SERIAL PRIMARY KEY,
            timestamp_start_ns BIGINT NOT NULL,
            timestamp_end_ns BIGINT NOT NULL,
            exchange_name VARCHAR(50),
            stream_id VARCHAR(100),
            predicted_exchange VARCHAR(50) NOT NULL,
            confidence FLOAT,
            num_messages INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Optional: store probabilities as JSON
            probabilities JSONB
        );
        
        -- Create index for faster queries
        CREATE INDEX IF NOT EXISTS idx_timestamp_start 
        ON classified_windows(timestamp_start_ns DESC);
        
        CREATE INDEX IF NOT EXISTS idx_predicted_exchange 
        ON classified_windows(predicted_exchange);
    """)
    
    with engine.begin() as conn:
        conn.execute(create_table_sql)
        print("✅ Database tables created successfully!")
    
    # Verify the table exists
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'classified_windows';
        """))
        tables = result.fetchall()
        print(f"📊 Tables in database: {[t[0] for t in tables]}")

if __name__ == "__main__":
    init_database()

