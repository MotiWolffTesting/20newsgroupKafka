## 20newsgroupsKafka

A small Kafka demo that publishes random samples from the 20 Newsgroups dataset into two topics and consumes them into MongoDB. Each service exposes a FastAPI HTTP interface.

### Prerequisites
- Docker
- Docker Compose

### Services
- **Zookeeper**: Backing service for Kafka (ports: 2181)
- **Kafka**: Message broker (ports: 9092 internal, 29092 external)
- **MongoDB**: Document store for consumed messages (port: 27017)
- **Publisher** (`services/publisher`): FastAPI app to publish messages
  - Port: 8000 (host)
  - Endpoints:
    - `GET /health`
    - `GET /publish`: sends one message per category into each topic
- **Subscriber - interesting** (`services/subscriber`): Consumes from `interesting` topic
  - Port: 8001 (host)
  - Endpoints: `GET /health`, `GET /messages/all`, `GET /messages/new`
- **Subscriber - not_interesting** (`services/subscriber`): Consumes from `not_interesting` topic
  - Port: 8002 (host; mapped to 8001 in container)
  - Endpoints: `GET /health`, `GET /messages/all`, `GET /messages/new`

### Topics
- **interesting**
- **not_interesting**

### Folder Structure
```
20newsgroupsKafka/
  docker-compose.yaml
  services/
    publisher/
      Dockerfile
      main.py
      requirements.txt
    subscriber/
      Dockerfile
      main.py
      requirements.txt
  shared/
    categories.py
  README.md
```

### Configuration (env)
- **Common**
  - `KAFKA_BOOTSTRAP_SERVERS` (default inside compose: `kafka:9092`)
- **Publisher**
  - `TOPIC_INTERESTING` (default: `interesting`)
  - `TOPIC_NOT_INTERESTING` (default: `not_interesting`)
- **Subscriber**
  - `KAFKA_CONSUMER_GROUP` (e.g., `subscriber-interesting`)
  - `KAFKA_TOPIC` (`interesting` or `not_interesting`)
  - `MONGO_URI` (default: `mongodb://mongo:27017` in compose)
  - `MONGO_DB` (default: `news20`)
  - `MONGO_COLLECTION` (defaults to `KAFKA_TOPIC`)

### Quick Start
1. Start services:
   ```bash
   docker compose up -d
   ```
2. Check health:
   ```bash
   curl -s http://localhost:8000/health | jq
   curl -s http://localhost:8001/health | jq
   curl -s http://localhost:8002/health | jq
   ```
3. Publish one message per category to each topic:
   ```bash
   curl -s http://localhost:8000/publish | jq
   ```
4. Read consumed messages:
   - From `interesting` subscriber (host port 8001):
     ```bash
     curl -s http://localhost:8001/messages/all | jq '.[0:3]'
     ```
   - From `not_interesting` subscriber (host port 8002):
     ```bash
     curl -s http://localhost:8002/messages/all | jq '.[0:3]'
     ```
5. Tail logs (optional):
   ```bash
   docker compose logs -f
   ```
6. Stop services:
   ```bash
   docker compose down
   ```

### Data Model
Documents stored in MongoDB include the original payload and metadata:
```json
{
  "category": "comp.graphics",
  "text": "...newsgroup post body...",
  "timestamp": "2024-01-01T12:34:56.789Z",
  "topic": "interesting"
}
```

### Notes
- The publisher samples one random post per category for both groups defined in `shared/categories.py`.
- Subscribers auto-create an index on `timestamp` and expose `messages/new` which returns only documents since your last call.
- External Kafka access for host tools is on `localhost:29092`.

### Troubleshooting
- **Kafka not ready**: The subscribers retry for ~90s on startup; check `docker compose logs kafka`.
- **No data in Mongo**: Ensure you called `GET /publish` and the subscribers are healthy.
- **Port conflicts**: Adjust published ports in `docker-compose.yaml` if 8000/8001/8002/27017 are in use.