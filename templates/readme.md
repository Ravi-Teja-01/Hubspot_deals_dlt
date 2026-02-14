# {{SERVICE_NAME_TITLE}} Data Extraction Service

A robust Flask-RESTX API service for extracting {{SERVICE_NAME_TITLE}} data using DLT (Data Load Tool) and PostgreSQL. Features comprehensive Swagger documentation, Docker support, and production-ready deployment.

## Features

- **{{SERVICE_NAME_TITLE}} API Integration**: Extracts data via {{SERVICE_NAME_TITLE}} REST API
- **DLT Integration**: Efficient data loading with PostgreSQL destination
- **Flask-RESTX**: RESTful API with automatic Swagger documentation
- **Async Processing**: Non-blocking scan operations with real-time status tracking
- **Multi-Environment Docker**: Separate configurations for dev/staging/prod
- **Comprehensive Monitoring**: Health checks, logging, and pipeline information
- **Data Validation**: Robust input validation using Marshmallow schemas
- **Production Ready**: Gunicorn WSGI server, proper error handling, CORS support

## Project Structure

```
{{SERVICE_NAME_KEBAB}}-extraction/
├── README.md
├── docker-compose.yml           # Multi-environment Docker orchestration
├── Dockerfile.dev              # Development container
├── Dockerfile.stage            # Staging container
├── Dockerfile.prod             # Production container
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variables template
├── .gitignore
├── .dockerignore
│
├── app.py                      # Flask application factory
├── wsgi.py                     # WSGI entry point
├── config.py                   # Configuration management
├── loki_logger.py              # Loki logging integration
├── utils.py                    # Utility functions
│
├── api/                        # API layer
│   ├── __init__.py
│   ├── routes.py               # Flask-RESTX API routes with Swagger
│   └── schemas.py              # Marshmallow validation schemas
│
├── services/                   # Business logic
│   ├── __init__.py
│   ├── extraction_service.py   # Main extraction service with DLT
│   └── api_service.py          # {{SERVICE_NAME_TITLE}} API client
│
├── models/                     # Data models
│   └── __init__.py
│
├── dev_scripts/                # Development utilities
│   └── __init__.py
│
├── docs/                       # Documentation
│   ├── api.md
│   └── deployment.md
│
└── logs/                       # Application logs
    └── .gitkeep
```

## Setup & Installation

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local development)
- {{SERVICE_NAME_TITLE}} API Access Token/Credentials

### Quick Start with Docker

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd {{SERVICE_NAME_KEBAB}}-extraction
   ```

2. **Set up environment variables**:
   ```bash
   cp .env.example .env
   # Edit .env with your {{SERVICE_NAME_TITLE}} API credentials
   ```

3. **Start services by environment**:

   **Development Environment (with tools):**
   ```bash
   # Start development services with pgAdmin and Redis Commander
   docker-compose --profile dev up -d
   
   # Start core development services only
   docker-compose up -d
   ```

   **Staging Environment:**
   ```bash
   # Start staging services
   docker-compose --profile stage up -d
   ```

   **Production Environment:**
   ```bash
   # Start production services
   docker-compose --profile prod up -d
   ```

4. **Verify the setup**:
   ```bash
   # Check service health
   curl http://localhost:{{PORT_DEV}}/health
   
   # View Swagger documentation
   open http://localhost:{{PORT_DEV}}/docs/
   
   # Access development tools (dev profile only)
   open http://localhost:8080/  # pgAdmin
   open http://localhost:8081/  # Redis Commander
   ```

### Local Development Setup

1. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables**:
   ```bash
   export FLASK_ENV=development
   export FLASK_DEBUG=true
   export DB_HOST=localhost
   export DB_PASSWORD=password123
   export {{SERVICE_NAME_UPPER}}_API_TOKEN="your-token-here"
   ```

3. **Start PostgreSQL** (using Docker):
   ```bash
   docker run -d --name {{SERVICE_NAME_SNAKE}}_postgres \
     -e POSTGRES_DB={{SERVICE_NAME_SNAKE}}_data_dev \
     -e POSTGRES_USER=postgres \
     -e POSTGRES_PASSWORD=password123 \
     -p 5432:5432 postgres:15-alpine
   ```

4. **Run the application**:
   ```bash
   python app.py
   ```

## API Documentation

### Swagger UI
Access the interactive API documentation at: **http://localhost:{{PORT_DEV}}/docs/**

### Available Endpoints

| Method | Endpoint | Description | Parameters |
|--------|----------|-------------|------------|
| `GET` | `/health` | Health check endpoint | None |
| `GET` | `/stats` | Service statistics | None |
| `GET` | `/docs/` | Swagger UI documentation | None |
| `POST` | `/scan/start` | Start a new data extraction scan | Body: scanId, organizationId, type, auth, filters |
| `GET` | `/scan/{scan_id}/status` | Get scan status | Path: scan_id |
| `POST` | `/scan/{scan_id}/cancel` | Cancel a running scan | Path: scan_id |
| `GET` | `/scan/list` | List all scans with pagination | Query: organizationId, limit, offset |
| `GET` | `/scan/statistics` | Get scan statistics | Query: organizationId |
| `DELETE` | `/scan/{scan_id}/remove` | Remove scan and data | Path: scan_id |
| `GET` | `/results/{scan_id}/tables` | Get available tables | Path: scan_id |
| `GET` | `/results/{scan_id}/result` | Get scan results | Path: scan_id; Query: tableName, limit, offset |
| `GET` | `/pipeline/info` | Get DLT pipeline info | None |
| `POST` | `/maintenance/cleanup` | Clean up old scans | Body: daysOld |
| `POST` | `/maintenance/detect-crashed` | Detect crashed jobs | Query: timeoutMinutes |

### Example API Usage

#### Start a Scan
```bash
curl -X POST http://localhost:{{PORT_DEV}}/api/v1/scan/start \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "scanId": "{{SERVICE_NAME_KEBAB}}-scan-001",
      "organizationId": "org-12345",
      "type": ["data"],
      "auth": {
        "accessToken": "your-{{SERVICE_NAME_KEBAB}}-token-here"
      },
      "filters": {
        "properties": ["id", "name", "status"],
        "includeArchived": false
      }
    }
  }'
```

#### Check Scan Status
```bash
curl http://localhost:{{PORT_DEV}}/api/v1/scan/{{SERVICE_NAME_KEBAB}}-scan-001/status
```

#### List All Scans
```bash
curl "http://localhost:{{PORT_DEV}}/api/v1/scan/list?organizationId=org-12345&limit=10"
```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLASK_ENV` | Flask environment | `development` |
| `FLASK_DEBUG` | Enable debug mode | `false` |
| `DB_HOST` | PostgreSQL host | `postgres_dev` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `{{SERVICE_NAME_SNAKE}}_data_dev` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD` | Database password | `password123` |
| `DB_SCHEMA` | Database schema | `{{SERVICE_NAME_SNAKE}}_dev` |
| `{{SERVICE_NAME_UPPER}}_API_TOKEN` | {{SERVICE_NAME_TITLE}} API token | `""` |
| `{{SERVICE_NAME_UPPER}}_API_TIMEOUT` | API timeout seconds | `30` |
| `{{SERVICE_NAME_UPPER}}_API_RATE_LIMIT` | API rate limit | `100` |
| `DLT_PIPELINE_NAME` | DLT pipeline name | `{{SERVICE_NAME_SNAKE}}_pipeline_dev` |
| `MAX_CONCURRENT_SCANS` | Max concurrent scans | `3` |
| `LOG_LEVEL` | Logging level | `INFO` |

### Multi-Environment Configuration

Each environment has its own configuration:

**Development:**
- Database: `{{SERVICE_NAME_SNAKE}}_data_dev`
- Port: `{{PORT_DEV}}`
- Debug mode enabled
- Hot reloading
- Development tools available

**Staging:**
- Database: `{{SERVICE_NAME_SNAKE}}_data_stage`
- Port: `{{PORT_STAGE}}`
- Production-like settings
- Staging-specific logging

**Production:**
- Database: `{{SERVICE_NAME_SNAKE}}_data_prod`
- Port: `{{PORT_PROD}}`
- Optimized performance
- Production logging
- Enhanced security

## Development

### Running Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_api.py -v
```

### Database Access
```bash
# Development database
docker-compose exec postgres_dev psql -U postgres -d {{SERVICE_NAME_SNAKE}}_data_dev

# View extracted data
SELECT * FROM {{SERVICE_NAME_SNAKE}}_dev.main_data LIMIT 10;
```

### Development Tools (Dev Profile)
- **pgAdmin**: http://localhost:8080 (admin@{{SERVICE_NAME_KEBAB}}.dev / admin123)
- **Redis Commander**: http://localhost:8081 (admin / admin123)

## Deployment

### Production Deployment
```bash
# Build and start production services
docker-compose --profile prod build
docker-compose --profile prod up -d

# Check production service
curl http://localhost:{{PORT_PROD}}/health
```

### Scaling
```bash
# Scale production service
docker-compose --profile prod up -d --scale {{SERVICE_NAME_SNAKE}}_service_prod=3
```

### Monitoring
```bash
# View logs
docker-compose logs -f {{SERVICE_NAME_SNAKE}}_service_dev

# Monitor resource usage
docker stats
```

## Troubleshooting

### Common Issues

**Service won't start:**
```bash
# Check logs
docker-compose logs {{SERVICE_NAME_SNAKE}}_service_dev

# Restart service
docker-compose restart {{SERVICE_NAME_SNAKE}}_service_dev
```

**Database connection issues:**
```bash
# Check database status
docker-compose exec postgres_dev pg_isready -U postgres

# Reset database
docker-compose down -v
docker-compose up -d
```

**{{SERVICE_NAME_TITLE}} API issues:**
- Verify API token in `.env` file
- Check {{SERVICE_NAME_TITLE}} API rate limits
- Validate API endpoint URLs

### Port Conflicts
If you encounter port conflicts, update the ports in your configuration and rebuild:

```bash
# Check port usage
netstat -tulpn | grep :{{PORT_DEV}}

# Update configuration and restart
docker-compose down
docker-compose up -d
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

Generated by DLT Generator - Customized for {{SERVICE_NAME_TITLE}} data extraction