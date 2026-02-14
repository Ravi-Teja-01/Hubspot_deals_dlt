# DLT Service Template - Guidelines & Step-by-Step Implementation

## Table of Contents
1. [Overview](#overview)
2. [Architecture Understanding](#architecture-understanding)
3. [Getting Started](#getting-started)
4. [Configuration Deep Dive](#configuration-deep-dive)
5. [Core Concepts](#core-concepts)
6. [Step-by-Step Implementation Guide](#step-by-step-implementation-guide)
7. [Production Deployment](#production-deployment)
8. [Troubleshooting](#troubleshooting)

---

## Overview

This template provides a production-ready foundation for building data extraction services using DLT (Data Load Tool). It handles all the infrastructure concerns so you can focus on your specific API integrations and data pipelines.

### What's Included Out-of-the-Box

✅ **Complete REST API** with Swagger documentation  
✅ **Job Management System** with status tracking and checkpointing  
✅ **Database Integration** with PostgreSQL and SQLAlchemy  
✅ **Docker Environment** for development and production  
✅ **Error Handling & Logging** with structured logging  
✅ **Multi-Tenant Support** using organizationId (tenantId)  
✅ **Health Checks & Monitoring** endpoints  

### What You Need to Customize

🔧 **API Service** - Your specific data source integration  
🔧 **Data Source** - Your DLT pipeline logic  
🔧 **Configuration** - Your service-specific settings  

---

## Architecture Understanding

### High-Level Flow

```
API Request → Flask Routes → ExtractionService → JobService (DB)
                                ↓
                           APIService (External API)
                                ↓
                           DataSource (DLT Pipeline)
                                ↓
                           PostgreSQL Database
```

### Key Components

| Component | Purpose | When to Modify |
|-----------|---------|----------------|
| **API Routes** | REST endpoints with validation | Rarely - only for new endpoints |
| **ExtractionService** | Main orchestration logic | Sometimes - for service-specific logic |
| **APIService** | External API integration | **Always** - customize for your API |
| **DataSource** | DLT pipeline definition | **Always** - define your data extraction |
| **JobService** | Job lifecycle management | Rarely - handles generic job operations |
| **DatabaseService** | Data querying after extraction | Sometimes - for custom query logic |

---

## Getting Started

### Step 1: Environment Setup

1. **Clone and Setup**
```bash
git clone <your-template-repo>
cd your-service-name
cp .env.example .env
```

2. **Configure Environment Variables** (see Configuration section below)

3. **Start Development Environment**
```bash
docker-compose up -d
```

4. **Verify Setup**
```bash
# Check health
curl http://localhost:5000/health

# View documentation
open http://localhost:5000/docs/
```

---

## Configuration Deep Dive

### Configuration Structure

The template uses a layered configuration system with environment-specific classes:

```python
# Base configuration for all environments
class Config:
    # Flask settings
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key')
    DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Database settings (PostgreSQL for DLT destination)
    DB_HOST = os.environ.get('DB_HOST', 'localhost')
    DB_PORT = int(os.environ.get('DB_PORT', 5432))
    DB_NAME = os.environ.get('DB_NAME', 'your_data')
    DB_USER = os.environ.get('DB_USER', 'postgres')
    DB_PASSWORD = os.environ.get('DB_PASSWORD', '')
    DB_SCHEMA = os.environ.get('DB_SCHEMA', 'your_schema')
```

### Environment-Specific Configurations

#### Development Config
```python
class DevelopmentConfig(Config):
    DEBUG = True
    # Use development database
    DB_NAME = os.environ.get('DB_NAME', 'your_data_dev')
    # Relaxed settings
    MAX_CONCURRENT_SCANS = 2
    LOG_LEVEL = 'DEBUG'
    # Allow all CORS origins
    CORS_ORIGINS = ['*']
```

#### Production Config
```python
class ProductionConfig(Config):
    DEBUG = False
    # Production optimizations
    DB_POOL_SIZE = 20
    DB_MAX_OVERFLOW = 40
    MAX_CONCURRENT_SCANS = 10
    LOG_LEVEL = 'INFO'
    
    # Security validations
    @classmethod
    def validate_production_config(cls):
        required_vars = ['SECRET_KEY', 'DB_PASSWORD']
        missing_vars = [var for var in required_vars if not getattr(cls, var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")
```

### Key Configuration Categories

#### 1. Service Identity Settings
```env
# Your service identification
DLT_PIPELINE_NAME=your_service_name        # This becomes your service name
DLT_WORKING_DIR=.dlt                       # DLT metadata storage
DLT_RUNTIME_ENV=production                 # DLT environment

# Flask application
SECRET_KEY=your-secret-32-char-key         # Required in production
PORT=5000
HOST=0.0.0.0
```

#### 2. Database Configuration (PostgreSQL)
```env
# PostgreSQL connection (DLT destination)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_service_data                  # Your service database
DB_USER=postgres
DB_PASSWORD=secure-password                # Required in production
DB_SCHEMA=your_schema                      # Schema for your data

# Connection pooling
DB_POOL_SIZE=10                           # Connection pool size
DB_MAX_OVERFLOW=20                        # Max overflow connections
DB_POOL_TIMEOUT=30                        # Connection timeout (seconds)
DB_POOL_RECYCLE=3600                      # Connection recycle time (seconds)
```

#### 3. External API Settings (Customize for Your Service)
```env
# Your API configuration
YOUR_API_BASE_URL=https://your-api.com
YOUR_API_TIMEOUT=30                       # Default timeout, can be overridden in request
YOUR_API_RATE_LIMIT=100                   # Default rate limit
YOUR_RETRY_ATTEMPTS=3                     # Default retry attempts
YOUR_RETRY_DELAY=1                        # Default retry delay

# Note: Authentication credentials come from API request, not environment
# This allows different tenants to use different credentials
```

#### 4. Service Operational Settings
```env
# Job management
MAX_CONCURRENT_SCANS=5                    # Max parallel extractions
SCAN_TIMEOUT_HOURS=24                     # Max scan duration
DEFAULT_BATCH_SIZE=100                    # Records per API call
CLEANUP_DAYS=7                           # Auto-cleanup old scans

# Tenant (Organization) Settings
# organizationId in API = tenantId in your system
TENANT_ISOLATION=true                     # Isolate tenant data
```

#### 5. Logging and Monitoring
```env
# Logging configuration
LOG_LEVEL=INFO                           # DEBUG, INFO, WARNING, ERROR
LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s
LOG_FILE_PATH=logs/app.log
LOG_MAX_BYTES=10485760                   # 10MB log files
LOG_BACKUP_COUNT=5                       # Keep 5 backup files

# Optional: Loki logging for centralized logs
LOKI_ENABLED=false
LOKI_URL=http://localhost:3100
LOKI_USERNAME=
LOKI_PASSWORD=
```

### Configuration Methods

#### 1. Environment-based Selection
```python
def get_config(config_name: str = None) -> Config:
    if not config_name:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    config_class = config_by_name.get(config_name, DevelopmentConfig)
    return config_class
```

#### 2. Service-Specific Config Helper
```python
@classmethod
def get_extraction_config(cls) -> Dict[str, Any]:
    """Get configuration for the extraction service"""
    return {
        # Database configuration
        'db_host': cls.DB_HOST,
        'db_port': cls.DB_PORT,
        'db_name': cls.DB_NAME,
        
        # DLT configuration
        'pipeline_name': cls.DLT_PIPELINE_NAME,
        'working_dir': cls.DLT_WORKING_DIR,
        
        # Your API configuration (customize this)
        'api_base_url': cls.YOUR_API_BASE_URL,
        'api_timeout': cls.YOUR_API_TIMEOUT,
        'retry_attempts': cls.YOUR_RETRY_ATTEMPTS,
    }
```

#### 3. DLT Environment Variables (Auto-generated)
```python
def build_dlt_env_vars(config: Dict[str, Any]) -> Dict[str, str]:
    """The template automatically sets these for DLT"""
    return {
        'DESTINATION__POSTGRES__CREDENTIALS__DATABASE': config.get('db_name'),
        'DESTINATION__POSTGRES__CREDENTIALS__USERNAME': config.get('db_user'),
        'DESTINATION__POSTGRES__CREDENTIALS__PASSWORD': config.get('db_password'),
        'DESTINATION__POSTGRES__CREDENTIALS__HOST': config.get('db_host'),
        'DESTINATION__POSTGRES__CREDENTIALS__PORT': str(config.get('db_port')),
    }
```

---

## Core Concepts

### 1. Job Lifecycle

Every data extraction follows this lifecycle:

```
PENDING → RUNNING → COMPLETED
    ↓         ↓         ↑
CANCELLED  FAILED   CRASHED
    ↓         ↓         ↓
  ENDED    ENDED   RESUMING → RUNNING
```

**Key States:**
- **PENDING**: Job created, waiting to start
- **RUNNING**: Actively extracting data
- **COMPLETED**: Successfully finished
- **FAILED**: Error occurred, job stopped
- **CRASHED**: Lost heartbeat, marked as crashed
- **RESUMING**: Restarting from last checkpoint

### 2. Tenant Isolation (organizationId)

The template treats `organizationId` as `tenantId` for multi-tenant support:

```python
# Dataset naming per tenant
dataset_name = build_dataset_name(job["organizationId"])  # e.g., "your_service_tenant123"

# Each tenant gets isolated data storage
pipeline = dlt.pipeline(
    pipeline_name=self.pipeline_name,
    destination=self.destination,
    dataset_name=dataset_name  # Isolated per tenant
)
```

### 3. Checkpointing System

Automatic checkpointing every N pages (configurable):

```python
# Checkpoint data structure
{
    'phase': 'data_type',                    # Current extraction phase
    'records_processed': 1500,               # Total records so far
    'cursor': 'next_page_token',             # API pagination cursor
    'page_number': 15,                       # Current page number
    'batch_size': 100,                       # Records per page
    'checkpoint_data': {                     # Custom metadata
        'current_item': 'specific_context',
        'last_processed_id': 'item_12345'
    }
}
```

---

## Step-by-Step Implementation Guide

Follow these exact steps to customize the template for your service:

### Step 1: Initial Setup and Environment

#### 1.1 Clone and Setup Repository
```bash
git clone <your-template-repo>
cd your-service-name
cp .env.example .env
```

#### 1.2 Edit `.env` File
Open `.env` and update these key values:

```env
# Change these for your service
DLT_PIPELINE_NAME=your_service_name      # Replace with your service name
DB_NAME=your_service_data                # Replace with your database name
DB_SCHEMA=your_schema                    # Replace with your schema name

# Your API base settings (credentials come from requests)
YOUR_API_BASE_URL=https://your-api.com   # Replace with your API URL
YOUR_API_TIMEOUT=30

# Keep these as-is for development
FLASK_ENV=development
FLASK_DEBUG=true
SECRET_KEY=dev-secret-key-change-in-production
DB_PASSWORD=password123
```

#### 1.3 Test Initial Setup
```bash
# Start the services
docker-compose up -d

# Wait 30 seconds, then test
curl http://localhost:5000/health

# Should return: {"status": "healthy", ...}
```

---

### Step 2: Customize Configuration

#### 2.1 Edit `config.py`
Open `config.py` and find the `Config` class. Add your service-specific settings:

**Find this section around line 50:**
```python
# HubSpot API Configuration
HUBSPOT_ACCESS_TOKEN=pat-na1-your-token-here
HUBSPOT_API_TIMEOUT=30
```

**Replace with your service settings:**
```python
# Your Service API settings
YOUR_API_BASE_URL = 'https://your-api.com'
YOUR_API_TIMEOUT = int(os.environ.get('YOUR_API_TIMEOUT', 30))
YOUR_API_RATE_LIMIT = int(os.environ.get('YOUR_API_RATE_LIMIT', 100))
YOUR_RETRY_ATTEMPTS = int(os.environ.get('YOUR_RETRY_ATTEMPTS', 3))

# Note: Authentication credentials come from request, not environment
```

#### 2.2 Update `get_extraction_config()` Method
**Find this method around line 200:**
```python
@classmethod
def get_extraction_config(cls) -> Dict[str, Any]:
    return {
        # HubSpot API configuration
        'hubspot_api_base_url': cls.HUBSPOT_API_BASE_URL,
        # ... other hubspot settings
    }
```

**Replace with:**
```python
@classmethod
def get_extraction_config(cls) -> Dict[str, Any]:
    return {
        # ... existing config (keep database, DLT settings) ...
        
        # Your API configuration
        'api_base_url': cls.YOUR_API_BASE_URL,
        'api_timeout': cls.YOUR_API_TIMEOUT,
        'retry_attempts': cls.YOUR_RETRY_ATTEMPTS,
        # Authentication credentials passed from request
    }
```

---

### Step 3: Create Your API Service

#### 3.1 Create New API Service File
Create `services/your_api_service.py`:

```bash
touch services/your_api_service.py
```

#### 3.2 Implement Your API Service Template
Copy this template into your new file and customize:

```python
import requests
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import time
import json


class YourAPIService:
    """
    Service for interacting with Your API
    """
    
    def __init__(self, base_url: str = "https://your-api.com"):
        self.base_url = base_url.rstrip('/')
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Your-Service-Extraction/1.0'
        })
    
    def authenticate(self, auth_config: Dict[str, Any]) -> str:
        """
        Authenticate and get access token
        Customize this based on your API's authentication method
        """
        # Example: API Key authentication
        api_key = auth_config.get('apiKey')
        if api_key:
            self.session.headers.update({'Authorization': f'Bearer {api_key}'})
            return api_key
            
        # Example: OAuth2 Client Credentials
        client_id = auth_config.get('clientId')
        client_secret = auth_config.get('clientSecret')
        if client_id and client_secret:
            token_url = f"{self.base_url}/oauth/token"
            data = {
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials'
            }
            response = self.session.post(token_url, data=data)
            response.raise_for_status()
            access_token = response.json()['access_token']
            self.session.headers.update({'Authorization': f'Bearer {access_token}'})
            return access_token
            
        raise ValueError("Invalid authentication configuration")
    
    def get_data(self, limit: int = 100, cursor: str = None, **filters) -> Dict[str, Any]:
        """
        Get data from your API with pagination
        Customize this based on your API's data structure
        """
        url = f"{self.base_url}/api/v1/your-endpoint"
        params = {'limit': min(limit, 1000)}
        
        if cursor:
            params['cursor'] = cursor
            
        # Add any filters
        params.update(filters)
        
        response = self.session.get(url, params=params)
        
        # Handle rate limiting
        if response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 60))
            self.logger.warning(f"Rate limited, retrying after {retry_after} seconds")
            time.sleep(retry_after)
            response = self.session.get(url, params=params)
        
        response.raise_for_status()
        return response.json()
    
    def validate_credentials(self, auth_config: Dict[str, Any]) -> bool:
        """Validate credentials by making a test API call"""
        try:
            self.authenticate(auth_config)
            self.get_data(limit=1)
            return True
        except Exception as e:
            self.logger.error(f"Credential validation failed: {e}")
            return False
    
    def get_account_info(self) -> Optional[Dict[str, Any]]:
        """Get basic account information"""
        try:
            url = f"{self.base_url}/api/v1/account"
            response = self.session.get(url)
            
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            self.logger.debug(f"Account info not available: {str(e)}")
            return None
```

---

### Step 4: Create Your Data Source

#### 4.1 Create New Data Source File
Replace `services/data_source.py` with your implementation:

```bash
# Backup original (optional)
mv services/data_source.py services/data_source.py.backup
```

#### 4.2 Implement Your Data Source Template
Create new `services/data_source.py`:

```python
import dlt
import logging
from typing import Dict, List, Any, Iterator, Optional, Callable
from datetime import datetime, timezone


def create_data_source(
    api_service, 
    auth_config: Dict[str, Any],
    filters: Dict[str, Any],
    checkpoint_callback: Optional[Callable] = None,
    resume_from: Optional[Dict[str, Any]] = None
):
    """
    Create DLT source function for data extraction with checkpoint support
    
    Args:
        api_service: API service instance
        auth_config: Authentication config from request
        filters: Extraction filters and configuration
        checkpoint_callback: Function to save checkpoints during extraction
        resume_from: Resume state from previous checkpoint
    """
    
    # Authenticate with your API
    access_token = api_service.authenticate(auth_config)
    
    @dlt.resource(name="your_data", write_disposition="replace", primary_key="id")
    def get_your_data() -> Iterator[Dict[str, Any]]:
        """Extract your data with checkpoint support"""
        logger = logging.getLogger(__name__)
        job_id = filters.get('scan_id', 'unknown')
        
        # Resume from checkpoint if available
        if resume_from:
            cursor = resume_from.get('cursor')
            page_count = resume_from.get('page_number', 0)
            total_records = resume_from.get('records_processed', 0)
            logger.info(f"Resuming from page {page_count + 1}, cursor: {cursor}")
        else:
            cursor = None
            page_count = 0
            total_records = 0
            logger.info("Starting fresh extraction")
        
        # Checkpoint configuration - save every N pages
        checkpoint_interval = 10  # Adjust based on your needs
        
        try:
            while page_count < 10000:  # Safety limit
                logger.debug(f"Fetching page {page_count + 1} (cursor: {cursor})")
                
                # Get data from your API
                data = api_service.get_data(
                    limit=100,
                    cursor=cursor,
                    **filters  # Pass any additional filters
                )
                
                page_records = 0
                
                # Process data records
                if 'results' in data and data['results']:
                    for record in data['results']:
                        # Add extraction metadata
                        record.update({
                            '_extracted_at': datetime.now(timezone.utc).isoformat(),
                            '_scan_id': filters.get('scan_id'),
                            '_tenant_id': filters.get('organization_id'),
                            '_page_number': page_count + 1
                        })
                        
                        yield record
                        page_records += 1
                        total_records += 1
                
                page_count += 1
                
                # Save checkpoint every N pages
                if checkpoint_callback and page_count % checkpoint_interval == 0:
                    try:
                        next_cursor = data.get('next_cursor') or data.get('paging', {}).get('next')
                        
                        checkpoint_data = {
                            'phase': 'your_data',
                            'records_processed': total_records,
                            'cursor': next_cursor,
                            'page_number': page_count,
                            'batch_size': 100,
                            'checkpoint_data': {
                                'pages_processed': page_count,
                                'last_page_records': page_records
                            }
                        }
                        
                        checkpoint_callback(job_id, checkpoint_data)
                        logger.info(f"Checkpoint saved at page {page_count}: {total_records} total records")
                        
                    except Exception as checkpoint_error:
                        logger.warning(f"Failed to save checkpoint: {checkpoint_error}")
                
                # Handle pagination
                if data.get('has_more') and data.get('next_cursor'):
                    cursor = data['next_cursor']
                else:
                    # Final checkpoint on completion
                    if checkpoint_callback:
                        try:
                            final_checkpoint = {
                                'phase': 'completed',
                                'records_processed': total_records,
                                'cursor': None,
                                'page_number': page_count,
                                'checkpoint_data': {
                                    'completion_status': 'success',
                                    'total_pages': page_count,
                                    'final_total': total_records
                                }
                            }
                            checkpoint_callback(job_id, final_checkpoint)
                            logger.info(f"Final checkpoint saved: {total_records} total records")
                        except Exception as e:
                            logger.warning(f"Failed to save final checkpoint: {e}")
                    
                    logger.info(f"Data extraction completed. Total records: {total_records}, Pages: {page_count}")
                    break
                    
        except Exception as e:
            logger.error(f"Error during extraction: {str(e)}")
            
            # Save error checkpoint for debugging
            if checkpoint_callback:
                try:
                    error_checkpoint = {
                        'phase': 'error',
                        'records_processed': total_records,
                        'cursor': cursor,
                        'page_number': page_count,
                        'checkpoint_data': {
                            'error': str(e),
                            'error_page': page_count + 1
                        }
                    }
                    checkpoint_callback(job_id, error_checkpoint)
                except:
                    pass
            
            raise e
    
    return [get_your_data]
```

---

### Step 5: Update Extraction Service

#### 5.1 Edit `services/extraction_service.py`
Find line ~15 where the API service is imported and initialized:

**Find:**
```python
from .api_service import APIService
```

**Replace with:**
```python
from .your_api_service import YourAPIService
```

**Find around line 30:**
```python
self.api_service = APIService()
```

**Replace with:**
```python
self.api_service = YourAPIService()
```

**Find around line 35:**
```python
def __init__(self, config: Dict[str, Any], source_type: str = "hubspot_users"):
```

**Replace with:**
```python
def __init__(self, config: Dict[str, Any], source_type: str = "your_service"):
```

---

### Step 6: Update API Schemas (Optional)

#### 6.1 Edit `api/schemas.py` for Custom Validation
Find the `AuthSchema` class and update for your auth requirements:

**Example for API Key auth:**
```python
class AuthSchema(Schema):
    """Authentication schema"""
    apiKey = fields.Str(
        required=True,
        validate=validate.Length(min=10),
        error_messages={'required': 'API key is required'}
    )
```

**Example for OAuth2 Client Credentials:**
```python
class AuthSchema(Schema):
    """Authentication schema"""
    clientId = fields.Str(
        required=True,
        validate=validate.Length(min=10),
        error_messages={'required': 'Client ID is required'}
    )
    clientSecret = fields.Str(
        required=True,
        validate=validate.Length(min=10),
        error_messages={'required': 'Client Secret is required'}
    )
```

---

### Step 7: Test Your Implementation

#### 7.1 Restart Services
```bash
# Stop services
docker-compose down

# Rebuild and start
docker-compose up -d --build

# Check logs
docker-compose logs -f your_service_name
```

#### 7.2 Test Health Check
```bash
curl http://localhost:5000/health
```

Should return:
```json
{
  "status": "healthy",
  "timestamp": "2025-01-XX...",
  "service": "your_service",
  "pipeline": { ... }
}
```

#### 7.3 Test API Documentation
Open browser: `http://localhost:5000/docs/`

#### 7.4 Test Scan Creation
```bash
curl -X POST http://localhost:5000/api/v1/scan/start \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "scanId": "test-scan-001",
      "organizationId": "test-tenant-123",
      "type": ["your_data_type"],
      "auth": {
        "apiKey": "your-test-api-key"
      },
      "filters": {
        "dateRange": {
          "startDate": "2024-01-01",
          "endDate": "2024-12-31"
        }
      }
    }
  }'
```

#### 7.5 Check Scan Status
```bash
curl http://localhost:5000/api/v1/scan/test-scan-001/status
```

---

## Production Deployment

### Environment Preparation

```bash
# Production environment
export FLASK_ENV=production
export SECRET_KEY=$(openssl rand -hex 32)
export DB_PASSWORD=secure-production-password

# Your API settings
export YOUR_API_BASE_URL=https://your-api.com
export YOUR_API_TIMEOUT=30

# Scale settings
export MAX_CONCURRENT_SCANS=10
export DB_POOL_SIZE=20
```

### Docker Production Setup

```yaml
# docker-compose.prod.yml
version: '3.8'
services:
  your_service:
    build:
      context: .
      dockerfile: Dockerfile.prod
    environment:
      - FLASK_ENV=production
      - SECRET_KEY=${SECRET_KEY}
      - YOUR_API_BASE_URL=${YOUR_API_BASE_URL}
      - YOUR_API_TIMEOUT=30
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2.0'
```

---

## Troubleshooting

### Common Issues & Solutions

#### 1. "Job already exists" Error
```bash
# Check job status first
curl http://localhost:5000/api/v1/scan/your-scan-id/status

# If crashed, it will auto-resume
# If completed, use a new scanId
```

#### 2. Authentication Failures
```bash
# Test your credentials manually with your API
curl -H "Authorization: Bearer your-token" \
     https://your-api.com/test-endpoint
```

#### 3. Database Connection Issues
```bash
# Check database health
curl http://localhost:5000/health

# Direct database test
docker exec -it postgres_container pg_isready
```

#### 4. Memory Issues with Large Datasets
- Use smaller batch sizes
- Implement more frequent checkpointing
- Consider processing in smaller time windows

#### 5. API Rate Limiting
```python
# Add exponential backoff in your API service
def api_call_with_retry(self, api_func, *args, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            return api_func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = (2 ** attempt) * 60
                time.sleep(wait_time)
            else:
                raise
```

---

## Quick Reference

### Key Files to Modify
1. **`.env`** - Service configuration
2. **`config.py`** - Add your API settings  
3. **`services/your_api_service.py`** - Create your API integration
4. **`services/data_source.py`** - Replace with your data extraction logic
5. **`services/extraction_service.py`** - Update imports and service type
6. **`api/schemas.py`** - Optional: Update auth validation

### API Request Pattern
```json
{
  "config": {
    "scanId": "unique-scan-id",
    "organizationId": "tenant123",
    "type": ["your_data_type"],
    "auth": {
      "your_auth_method": "credentials"
    },
    "filters": {
      "your_custom_filters": "values"
    }
  }
}
```

### Database Tables Created
- `jobs` - Job status and metadata
- `job_checkpoints` - Checkpointing data  
- `your_schema.your_data` - Extracted data (tenant-isolated)

---

## Next Steps

1. **✅ Follow the step-by-step guide exactly** - Don't skip any steps
2. **🧪 Test with small dataset first** - Use a test tenant with limited data
3. **📊 Monitor the extraction** - Watch logs and health endpoints
4. **🔧 Customize further** - Add your specific business logic
5. **🚀 Deploy to production** - Use production configuration

**Remember**: Focus on your API integration and data extraction logic. The template handles all the infrastructure concerns!