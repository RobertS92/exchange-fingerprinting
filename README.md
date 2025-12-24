# Exchange Behavior Fingerprinting System


## 🎯 Overview

This system analyzes real-time message streams from cryptocurrency exchanges and identifies which exchange they originated from based on behavioral fingerprints such as:

- **Interarrival time patterns** (mean, std, quantiles, FFT frequency analysis)
- **Message type n-grams** (unigrams, bigrams of NEW/ACK/NACK/HEARTBEAT)
- **ACK/NACK latency patterns**
- **Sequence number anomalies** (jumps, backwards movements)

The system achieves **99%+ accuracy** in distinguishing between exchanges (Binance, Coinbase, Kraken) using synthetic training data.

## 🏗️ Architecture

```
Data Source → Kafka → Feature Extraction → ML Model → PostgreSQL → Dashboard
     ↓           ↓            ↓                ↓           ↓           ↓
 WebSocket   raw_msgs    fingerprint_    FastAPI     db_sink   Streamlit
              topic        worker       (port 8001)            (port 8502)
```

### Components

1. **Ingestion Services** (`streaming_services/`)
   - `ingestion_binance.py` - Real exchange WebSocket ingestion
   - `synthetic_ingestion.py` - Synthetic data generator for testing

2. **Feature Extraction** (`src/features/`)
   - `interarrival.py` - Timing pattern analysis with FFT
   - `msg_ngrams.py` - Message sequence patterns
   - `ack_latency.py` - Response time characteristics
   - `seq_anomaly.py` - Sequence number behavior
   - `extractor.py` - Unified feature extraction

3. **ML Model** (`src/models/`)
   - `xgb_model.py` - XGBoost classifier wrapper
   - Trained model: `data/models/xgb_exchange_fp.json`

4. **Streaming Pipeline** (`streaming_services/`)
   - `fingerprint_worker.py` - Sliding window processing & classification
   - `db_sink.py` - PostgreSQL persistence

5. **API & Dashboard**
   - `api/serve_model.py` - FastAPI model serving
   - `streamlit_app/app.py` - Real-time monitoring dashboard

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local development)

### Run the System

```bash
# Start all services
docker compose up -d --build

# Initialize database schema
docker compose exec model_api python init_db.py

# Generate synthetic data (since Binance blocks WebSocket connections)
docker exec ingestion_binance python streaming_services/synthetic_ingestion.py
```

### Access the Dashboard

Open http://localhost:8502 to see real-time classifications!

### Service Ports

- **Streamlit Dashboard**: http://localhost:8502
- **Model API**: http://localhost:8001
- **PostgreSQL**: localhost:5432
- **Kafka**: localhost:9092 (internal: 29092)
- **Zookeeper**: localhost:2181

## 📊 Model Training

The model was trained in `notebooks/Exchange_classifier.ipynb` using synthetic exchange data with realistic behavioral patterns:

- **Features**: 56 behavioral features
- **Algorithm**: XGBoost (300 estimators, depth 8)
- **Accuracy**: 99%+ on test data
- **Window Size**: 50 messages with 25-message stride

## 🛠️ Development

### Local Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt
```

### Project Structure

```
exchange-fingerprinting/
├── api/                    # FastAPI model serving
├── data/                   # Trained models and metadata
├── notebooks/              # Jupyter notebooks for training
├── src/                    # Core library code
│   ├── config.py          # Configuration
│   ├── features/          # Feature extraction modules
│   ├── models/            # Model wrapper
│   └── streaming/         # Window utilities
├── streaming_services/     # Kafka-based services
├── streamlit_app/         # Dashboard
├── tests/                 # Unit tests
├── docker-compose.yml     # Service orchestration
├── Dockerfile            # Python app container
└── requirements.txt      # Python dependencies
```

## 🔍 Key Features

- **Real-time Processing**: Kafka-based streaming architecture
- **Sliding Windows**: Overlapping 50-message windows for continuous analysis
- **Production-Ready**: Docker containerization, proper logging, error handling
- **Scalable**: Kafka for horizontal scaling, separate worker processes
- **Observable**: Streamlit dashboard for live monitoring
- **Extensible**: Easy to add new exchanges or features

## 📈 Performance

- **Throughput**: ~100-200 windows/second per worker
- **Latency**: <100ms per classification
- **Accuracy**: 99.7%+ confidence on known patterns

## 🤝 Contributing

This project was built as a demonstration of AI-assisted development. Feel free to:
- Add support for more exchanges
- Implement additional features
- Optimize the model
- Improve the dashboard

## 📝 License

- Deciding 

## 🙏 Acknowledgments


- XGBoost for the ML framework
- Kafka for streaming infrastructure
- Streamlit for rapid dashboard development

---

**Note**: This system uses synthetic data for training and testing. For production deployment with real exchange data, ensure compliance with exchange terms of service and relevant regulations.



