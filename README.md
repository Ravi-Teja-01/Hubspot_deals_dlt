# DLT Generator

A powerful command-line tool for generating Data Load Tool (DLT) extraction services from templates. Create production-ready data pipeline services for any API quickly and consistently.

## ✨ Features

- **Template-Based Generation**: Create DLT services from customizable templates
- **Multi-Environment Support**: Automatic Docker configurations for dev, staging, and production
- **Smart Port Management**: Auto-generated or custom port assignments per environment
- **Placeholder System**: Dynamic service name and configuration replacement
- **Configuration-Driven**: JSON-based configuration for reproducible builds
- **Production Ready**: Includes Docker Compose, health checks, logging, and error handling

## 🚀 Quick Start

### 1. Installation

```bash
# Clone or download the DLT Generator
git clone <repository-url>

# Ensure Python 3.8+ is installed
python --version
```

### 2. First Run (Auto-Config Creation)

```bash
# Creates a sample config.json file
python dlt_generator.py
```

### 3. Configure Your Service

Edit the generated `config.json`:

```json
{
  "project_name": "salesforce-etl",
  "service_name": "salesforce",
  "template_path": "./template",
  "destination_dir": "./projects",
  "ports": {
    "dev": 5100,
    "stage": 5101,
    "prod": 5102
  },
  "force_overwrite": false,
  "verbose": false
}
```

### 4. Generate Your Service

```bash
# Generate the DLT service
python dlt_generator.py

# Or use a custom config file
python dlt_generator.py -c my-salesforce-config.json
```

## 📁 Template Structure

The tool uses a template folder structure that gets copied and customized:

```
template/
├── api/                    # API integration modules
├── services/               # Service layer implementations
├── models/                 # Data models and schemas
├── docs/                   # Documentation templates
├── dev_scripts/            # Development utilities
├── logs/                   # Log directory
├── .dlt/                   # DLT configuration
├── docker-compose.yml      # Multi-environment setup
├── Dockerfile.dev          # Development container
├── Dockerfile.stage        # Staging container
├── Dockerfile.prod         # Production container
├── app.py                  # Main application
├── config.py               # Configuration management
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variables template
└── README.md               # Generated service documentation
```

## 🎯 Placeholder System

The generator replaces placeholders throughout your template files:

### Service Name Placeholders

- `{{SERVICE_NAME}}` → `salesforce`
- `{{SERVICE_NAME_LOWER}}` → `salesforce`
- `{{SERVICE_NAME_UPPER}}` → `SALESFORCE`
- `{{SERVICE_NAME_TITLE}}` → `Salesforce`
- `{{SERVICE_NAME_SNAKE}}` → `salesforce`
- `{{SERVICE_NAME_KEBAB}}` → `salesforce`

### Port Placeholders

- `{{PORT_DEV}}` → `5100` (development port)
- `{{PORT_STAGE}}` → `5101` (staging port)
- `{{PORT_PROD}}` → `5102` (production port)

### Example Template Usage

```yaml
# docker-compose.yml template
name: {{SERVICE_NAME_TITLE}} DLT Extraction Service

services:
  {{SERVICE_NAME_SNAKE}}_service_dev:
    container_name: {{SERVICE_NAME_SNAKE}}_service_dev
    ports:
      - "{{PORT_DEV}}:{{PORT_DEV}}"
    environment:
      - DLT_PIPELINE_NAME={{SERVICE_NAME_SNAKE}}_pipeline_dev
```

## ⚙️ Configuration Reference

### Required Fields

| Field | Description | Example |
|-------|-------------|---------|
| `project_name` | Name of the generated project folder | `"salesforce-etl"` |
| `service_name` | Service name for placeholder replacement | `"salesforce"` |

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `template_path` | string | `"./template"` | Path to template folder |
| `destination_dir` | string | `"./"` | Where to create the project |
| `ports` | object | auto-generated | Port assignments for environments |
| `ports.dev` | integer | auto | Development port (1024-65535) |
| `ports.stage` | integer | auto | Staging port (1024-65535) |
| `ports.prod` | integer | auto | Production port (1024-65535) |
| `force_overwrite` | boolean | `false` | Skip overwrite prompts |
| `verbose` | boolean | `false` | Enable debug logging |

## 🐳 Multi-Environment Docker Support

Generated services include complete Docker configurations:

### Development Environment
```bash
# Start development services
docker-compose up

# With development tools (pgAdmin, Redis Commander)
docker-compose --profile dev up
```

### Staging Environment
```bash
# Start staging services
docker-compose --profile stage up postgres_stage salesforce_service_stage
```

### Production Environment
```bash
# Start production services
docker-compose --profile prod up postgres_prod salesforce_service_prod
```

## 📊 Generated Service Features

Each generated DLT service includes:

- **Multi-Environment Setup**: Separate configurations for dev/stage/prod
- **Database Integration**: PostgreSQL with environment-specific instances
- **Caching Layer**: Redis for session storage and caching
- **Health Checks**: Built-in health monitoring endpoints
- **Logging**: Structured logging with configurable levels
- **Error Handling**: Comprehensive error handling and recovery
- **API Templates**: Customizable API service templates with TODO guidance
- **DLT Integration**: Pre-configured DLT sources with checkpoint support
- **Development Tools**: pgAdmin and Redis Commander for development

## 🛠️ Customizing Templates

### API Service Template

The generated API service includes comprehensive TODO comments:

```python
class APIService:
    """Service for interacting with {{SERVICE_NAME_TITLE}} APIs"""
    
    def get_data(self, access_token: str, **kwargs) -> Dict[str, Any]:
        """
        TODO: Implement the following steps:
        1. Set up authentication headers
        2. Build request parameters based on {{SERVICE_NAME_TITLE}} API
        3. Handle pagination and rate limiting
        4. Parse and return response data
        """
```

### DLT Source Template

The DLT source template provides guidance for data extraction:

```python
@dlt.resource(name="main_data", write_disposition="replace", primary_key="id")
def get_main_data() -> Iterator[Dict[str, Any]]:
    """
    Extract main data from {{SERVICE_NAME_TITLE}} API
    
    TODO: Customize this resource for {{SERVICE_NAME_TITLE}}:
    1. Update resource name and primary key
    2. Modify API calls for specific endpoints
    3. Adjust pagination logic
    4. Add data transformation logic
    """
```

## 🌐 Generated API Endpoints

Each generated DLT service includes a comprehensive REST API based on the template structure:

### Scan Operations (`/api/v1/scan`)

| Method | Endpoint | Description | Parameters |
|--------|----------|-------------|------------|
| `POST` | `/scan/start` | Start a new data extraction scan | Body: scanId, organizationId, type, auth, filters |
| `GET` | `/scan/{scan_id}/status` | Get the status of a specific scan | Path: scan_id |
| `POST` | `/scan/{scan_id}/cancel` | Cancel a running scan | Path: scan_id |
| `GET` | `/scan/list` | List all scans with filtering and pagination | Query: organizationId, limit (max 100), offset |
| `GET` | `/scan/statistics` | Get scan statistics | Query: organizationId |
| `DELETE` | `/scan/{scan_id}/remove` | Remove a scan and its data | Path: scan_id |

### Results Operations (`/api/v1/results`)

| Method | Endpoint | Description | Parameters |
|--------|----------|-------------|------------|
| `GET` | `/results/{scan_id}/tables` | Get available tables for completed scan | Path: scan_id |
| `GET` | `/results/{scan_id}/result` | Retrieve scan results with table selection | Path: scan_id; Query: tableName, limit (max 1000), offset |

### Pipeline Operations (`/api/v1/pipeline`)

| Method | Endpoint | Description | Parameters |
|--------|----------|-------------|------------|
| `GET` | `/pipeline/info` | Get DLT pipeline configuration info | None |

### Maintenance Operations (`/api/v1/maintenance`)

| Method | Endpoint | Description | Parameters |
|--------|----------|-------------|------------|
| `POST` | `/maintenance/cleanup` | Clean up old scan results | Body: daysOld |
| `POST` | `/maintenance/detect-crashed` | Detect and mark crashed jobs | Query: timeoutMinutes (1-1440) |

### System Operations

| Method | Endpoint | Description | Parameters |
|--------|----------|-------------|------------|
| `GET` | `/health` | Health check endpoint | None |
| `GET` | `/stats` | Get service statistics | None |
| `GET` | `/docs/` | Swagger API documentation | None |

### API Features

Each generated service includes:
- **Swagger UI**: Available at `/docs/` for interactive API testing
- **Request Validation**: Automatic validation using Marshmallow schemas
- **Error Handling**: Consistent error responses with proper HTTP status codes
- **Pagination**: Built-in pagination for list endpoints with limit/offset
- **Authentication**: Token-based authentication support

### Multiple Services

Create different configurations for various services:

```bash
# Generate multiple services
python dlt_generator.py -c salesforce-config.json
python dlt_generator.py -c hubspot-config.json
python dlt_generator.py -c stripe-config.json
```

### Custom Templates

Use your own template folder:

```json
{
  "project_name": "my-custom-service",
  "service_name": "myapi",
  "template_path": "./my-custom-template"
}
```

### Port Management

```json
{
  "ports": {
    "dev": 5200,
    "stage": 5201,
    "prod": 5202
  }
}
```

Or let the generator auto-assign consistent ports based on service name.

## 📝 Command Line Options

```bash
# Use default config.json
python dlt_generator.py

# Use custom config file
python dlt_generator.py -c my-config.json

# Show version
python dlt_generator.py --version

# Help
python dlt_generator.py --help
```

## 🔍 Troubleshooting

### Common Issues

**Config file not found:**
```bash
# The tool automatically creates config.json if it doesn't exist
python dlt_generator.py
```

**Invalid JSON:**
```bash
# Check JSON syntax with verbose mode
python dlt_generator.py -c config.json
```

**Port conflicts:**
- Ensure ports are unique across services
- Use port ranges 3000-65535
- Check for conflicts with system services

**Permission errors:**
- Ensure write permissions to destination directory
- Run with appropriate user permissions
- Check disk space availability

### Verbose Mode

Enable detailed logging for debugging:

```json
{
  "verbose": true
}
```

## 📚 Examples

### Salesforce Integration

```json
{
  "project_name": "salesforce-crm-etl",
  "service_name": "salesforce",
  "destination_dir": "./integrations",
  "ports": {
    "dev": 5300,
    "stage": 5301,
    "prod": 5302
  }
}
```

### E-commerce Platform

```json
{
  "project_name": "shopify-orders-pipeline",
  "service_name": "shopify",
  "destination_dir": "./ecommerce",
  "ports": {
    "dev": 5400,
    "stage": 5401,
    "prod": 5402
  }
}
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch
3. Make your changes
4. Test with different service configurations
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

- Create issues for bugs or feature requests
- Check existing issues for solutions
- Provide detailed error messages and configuration when reporting issues

---

**DLT Generator v2.0.0** - Generate production-ready data pipeline services with ease!