# HubSpot Tickets Extraction - Complete Implementation Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step-by-Step Implementation](#step-by-step-implementation)
4. [API Configuration](#api-configuration)
5. [Code Implementation](#code-implementation)
6. [Testing & Validation](#testing--validation)
7. [Production Deployment](#production-deployment)
8. [Troubleshooting](#troubleshooting)

---

## Overview

This guide shows you how to implement HubSpot ticket extraction using the DLT Service Template. The implementation extracts support tickets and their associated data from HubSpot using the CRM API.

### What This Implementation Does

✅ **Extracts tickets** from HubSpot CRM  
✅ **Gets ticket properties** and custom fields  
✅ **Retrieves associated data** (contacts, companies, deals)  
✅ **Handles pagination** with HubSpot's after-cursor system  
✅ **Implements checkpointing** to resume from failures  
✅ **Supports rate limiting** with automatic retry  
✅ **Tenant isolation** - each HubSpot portal gets separate data storage  

### Data Flow

```
1. Authenticate with HubSpot Private App token
2. Get ticket properties schema
3. Extract tickets with all properties (paginated)
4. For each ticket, get associated records:
   - Contact information
   - Company information  
   - Deal information (if applicable)
5. Save checkpoint every 10 pages
6. Store in PostgreSQL with tenant isolation
```

### Database Schema Result

After extraction, you'll have these tables in schema `hubspot_tickets_portalX`:

- **`ticket_properties`** - Available ticket properties and their definitions
- **`tickets`** - All tickets with properties and metadata
- **`ticket_associations`** - Relationships between tickets and other CRM objects

---

## Prerequisites

### 1. HubSpot Private App Setup

You need to create a Private App in HubSpot with the required scopes:

#### Required Scopes
- **CRM**: `tickets` (Read)
- **CRM**: `crm.objects.contacts.read` (if getting contact associations)
- **CRM**: `crm.objects.companies.read` (if getting company associations)
- **CRM**: `crm.objects.deals.read` (if getting deal associations)

#### Setup Steps
1. Go to your HubSpot account
2. Navigate to **Settings** > **Integrations** > **Private Apps**
3. Click **Create a private app**
4. **Basic Info**:
   - Name: `Ticket Extraction Service`
   - Description: `Extract support tickets and associated data`

5. **Scopes** tab:
   - Check `tickets` under **CRM**
   - Check `crm.objects.contacts.read` under **CRM** 
   - Check `crm.objects.companies.read` under **CRM**
   - Check `crm.objects.deals.read` under **CRM** (optional)

6. Click **Create app**
7. **Copy the Access Token** - this is your `accessToken`

### 2. Get Portal ID
1. In HubSpot, go to **Settings** > **Account Setup** > **Account Defaults**
2. Copy the **Hub ID** - this will be your `organizationId` (tenant identifier)

---

## Step-by-Step Implementation

### Step 1: Initial Setup

#### 1.1 Clone and Configure Environment
```bash
git clone <your-template-repo>
cd hubspot-tickets-extraction
cp .env.example .env
```

#### 1.2 Edit `.env` File
```env
# Service Configuration
DLT_PIPELINE_NAME=hubspot_tickets_extraction
DB_NAME=hubspot_tickets_data
DB_SCHEMA=hubspot_tickets

# HubSpot API Base Settings  
HUBSPOT_API_BASE_URL=https://api.hubapi.com
HUBSPOT_API_TIMEOUT=30

# Development Settings
FLASK_ENV=development
FLASK_DEBUG=true
SECRET_KEY=dev-secret-key-change-in-production
DB_PASSWORD=password123
```

#### 1.3 Start and Test Base Setup
```bash
# Start services
docker-compose up -d

# Wait 30 seconds, then test
curl http://localhost:5000/health

# Should return healthy status
```

---

### Step 2: Update Configuration

#### 2.1 Edit `config.py`
Open `config.py` and find the HubSpot configuration section around line 50:

**Update the existing HubSpot settings:**
```python
# HubSpot API Configuration
HUBSPOT_API_BASE_URL = 'https://api.hubapi.com'
HUBSPOT_API_TIMEOUT = int(os.environ.get('HUBSPOT_API_TIMEOUT', 30))
HUBSPOT_API_RATE_LIMIT = int(os.environ.get('HUBSPOT_API_RATE_LIMIT', 100))
HUBSPOT_RETRY_ATTEMPTS = int(os.environ.get('HUBSPOT_RETRY_ATTEMPTS', 3))
HUBSPOT_RETRY_DELAY = int(os.environ.get('HUBSPOT_RETRY_DELAY', 1))

# HubSpot CRM API endpoints
HUBSPOT_TICKETS_ENDPOINT = '/crm/v3/objects/tickets'
HUBSPOT_PROPERTIES_ENDPOINT = '/crm/v3/properties/tickets'
HUBSPOT_ASSOCIATIONS_ENDPOINT = '/crm/v4/associations/tickets'

# Note: Access token comes from request, not environment
```

#### 2.2 Update `get_extraction_config()` Method
Find the method around line 200 and update:

```python
@classmethod
def get_extraction_config(cls) -> Dict[str, Any]:
    return {
        # Keep existing database and DLT settings...
        'db_host': cls.DB_HOST,
        'db_port': cls.DB_PORT,
        'db_name': cls.DB_NAME,
        'db_user': cls.DB_USER,
        'db_password': cls.DB_PASSWORD,
        'db_schema': cls.DB_SCHEMA,
        
        # DLT configuration
        'pipeline_name': cls.DLT_PIPELINE_NAME,
        'working_dir': cls.DLT_WORKING_DIR,
        
        # HubSpot API configuration
        'hubspot_api_base_url': cls.HUBSPOT_API_BASE_URL,
        'hubspot_api_timeout': cls.HUBSPOT_API_TIMEOUT,
        'hubspot_retry_attempts': cls.HUBSPOT_RETRY_ATTEMPTS,
        'hubspot_tickets_endpoint': cls.HUBSPOT_TICKETS_ENDPOINT,
        'hubspot_properties_endpoint': cls.HUBSPOT_PROPERTIES_ENDPOINT,
        'hubspot_associations_endpoint': cls.HUBSPOT_ASSOCIATIONS_ENDPOINT,
        # access_token passed from request
    }
```

---

### Step 3: Create HubSpot Tickets API Service

#### 3.1 Create API Service File
```bash
touch services/hubspot_tickets_api_service.py
```

#### 3.2 Implement HubSpot Tickets API Service
Copy this complete implementation into `services/hubspot_tickets_api_service.py`:

```python
import requests
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
import time
import json


class HubSpotTicketsAPIService:
    """
    Service for interacting with HubSpot CRM API for ticket extraction
    """
    
    def __init__(self, base_url: str = "https://api.hubapi.com"):
        self.base_url = base_url.rstrip('/')
        self.logger = logging.getLogger(__name__)
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'HubSpot-Tickets-Extraction-Service/1.0'
        })
    
    def set_access_token(self, token: str):
        """Set the HubSpot API access token"""
        self.session.headers.update({
            'Authorization': f'Bearer {token}'
        })
    
    def get_ticket_properties(self, access_token: str) -> Dict[str, Any]:
        """Get all available ticket properties and their definitions"""
        self.set_access_token(access_token)
        
        url = f"{self.base_url}/crm/v3/properties/tickets"
        
        try:
            self.logger.debug("Fetching ticket properties")
            response = self.session.get(url)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('X-HubSpot-RateLimit-Interval-Milliseconds', 1000)) / 1000
                self.logger.warning(f"Rate limited getting ticket properties, retrying after {retry_after} seconds")
                time.sleep(retry_after)
                response = self.session.get(url)
            
            response.raise_for_status()
            data = response.json()
            
            self.logger.info(f"Retrieved {len(data.get('results', []))} ticket properties")
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get ticket properties: {e}")
            raise
    
    def get_tickets(self, access_token: str, limit: int = 100, after: str = None, 
                   properties: List[str] = None, associations: List[str] = None) -> Dict[str, Any]:
        """Get tickets from HubSpot CRM with pagination"""
        self.set_access_token(access_token)
        
        url = f"{self.base_url}/crm/v3/objects/tickets"
        
        # Default properties if none specified
        if not properties:
            properties = [
                'hs_ticket_id', 'subject', 'content', 'hs_ticket_category',
                'hs_ticket_priority', 'hs_pipeline', 'hs_pipeline_stage',
                'hubspot_owner_id', 'source_type', 'hs_created_by_user_id',
                'createdate', 'hs_lastmodifieddate', 'closed_date'
            ]
        
        params = {
            'limit': min(limit, 100),  # HubSpot max is 100
            'properties': ','.join(properties)
        }
        
        if after:
            params['after'] = after
            
        if associations:
            params['associations'] = ','.join(associations)
        
        try:
            self.logger.debug(f"Fetching tickets with limit: {limit}, after: {after}")
            response = self.session.get(url, params=params)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('X-HubSpot-RateLimit-Interval-Milliseconds', 1000)) / 1000
                self.logger.warning(f"Rate limited getting tickets, retrying after {retry_after} seconds")
                time.sleep(retry_after)
                response = self.session.get(url, params=params)
            
            response.raise_for_status()
            data = response.json()
            
            ticket_count = len(data.get('results', []))
            self.logger.debug(f"Retrieved {ticket_count} tickets")
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get tickets: {e}")
            raise
    
    def get_ticket_associations(self, access_token: str, ticket_id: str, 
                              to_object_type: str) -> Dict[str, Any]:
        """Get associations for a specific ticket"""
        self.set_access_token(access_token)
        
        url = f"{self.base_url}/crm/v4/objects/tickets/{ticket_id}/associations/{to_object_type}"
        
        try:
            self.logger.debug(f"Fetching associations for ticket {ticket_id} to {to_object_type}")
            response = self.session.get(url)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('X-HubSpot-RateLimit-Interval-Milliseconds', 1000)) / 1000
                self.logger.warning(f"Rate limited getting associations, retrying after {retry_after} seconds")
                time.sleep(retry_after)
                response = self.session.get(url)
            
            # Handle 404 - no associations found
            if response.status_code == 404:
                return {'results': []}
            
            response.raise_for_status()
            data = response.json()
            
            association_count = len(data.get('results', []))
            self.logger.debug(f"Retrieved {association_count} associations for ticket {ticket_id}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get associations for ticket {ticket_id}: {e}")
            raise
    
    def get_contact(self, access_token: str, contact_id: str) -> Optional[Dict[str, Any]]:
        """Get contact details by ID"""
        self.set_access_token(access_token)
        
        url = f"{self.base_url}/crm/v3/objects/contacts/{contact_id}"
        params = {
            'properties': 'email,firstname,lastname,phone,company,jobtitle,lifecyclestage'
        }
        
        try:
            response = self.session.get(url, params=params)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('X-HubSpot-RateLimit-Interval-Milliseconds', 1000)) / 1000
                time.sleep(retry_after)
                response = self.session.get(url, params=params)
            
            # Handle 404 - contact not found or no access
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Could not get contact {contact_id}: {e}")
            return None
    
    def get_company(self, access_token: str, company_id: str) -> Optional[Dict[str, Any]]:
        """Get company details by ID"""
        self.set_access_token(access_token)
        
        url = f"{self.base_url}/crm/v3/objects/companies/{company_id}"
        params = {
            'properties': 'name,domain,industry,city,state,country,num_employees,annualrevenue'
        }
        
        try:
            response = self.session.get(url, params=params)
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('X-HubSpot-RateLimit-Interval-Milliseconds', 1000)) / 1000
                time.sleep(retry_after)
                response = self.session.get(url, params=params)
            
            # Handle 404 - company not found or no access
            if response.status_code == 404:
                return None
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Could not get company {company_id}: {e}")
            return None
    
    def validate_credentials(self, access_token: str) -> bool:
        """Validate HubSpot access token by making a test API call"""
        try:
            self.logger.info("Validating HubSpot access credentials...")
            
            # Test by getting ticket properties (lightweight call)
            self.get_ticket_properties(access_token)
            
            self.logger.info("Credentials validation successful")
            return True
            
        except Exception as e:
            self.logger.error(f"Credential validation failed: {e}")
            return False
    
    def get_account_info(self, access_token: str) -> Optional[Dict[str, Any]]:
        """Get HubSpot account/portal information"""
        self.set_access_token(access_token)
        
        try:
            url = f"{self.base_url}/account-info/v3/details"
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.debug("Retrieved account info")
                return data
            else:
                self.logger.debug(f"Account info not available: {response.status_code}")
                return None
                
        except Exception as e:
            self.logger.debug(f"Account info not available: {str(e)}")
            return None
    
    def get_api_usage(self, access_token: str) -> Optional[Dict[str, Any]]:
        """Get API usage information from response headers"""
        try:
            # Make a lightweight API call to get rate limit headers
            self.set_access_token(access_token)
            url = f"{self.base_url}/crm/v3/properties/tickets"
            params = {'limit': 1}
            
            response = self.session.get(url, params=params)
            
            if response.status_code == 200:
                usage_info = {}
                
                # Extract HubSpot rate limit headers
                rate_limit_headers = [
                    'X-HubSpot-RateLimit-Daily',
                    'X-HubSpot-RateLimit-Daily-Remaining', 
                    'X-HubSpot-RateLimit-Interval-Milliseconds',
                    'X-HubSpot-RateLimit-Max',
                    'X-HubSpot-RateLimit-Remaining'
                ]
                
                for header in rate_limit_headers:
                    if header in response.headers:
                        usage_info[header] = response.headers[header]
                
                if usage_info:
                    usage_info['timestamp'] = datetime.now(timezone.utc).isoformat()
                    return usage_info
                    
            return None
            
        except Exception as e:
            self.logger.debug(f"API usage info not available: {str(e)}")
            return None
```

---

### Step 4: Create Data Source

#### 4.1 Replace Data Source File
```bash
# Backup original
mv services/data_source.py services/data_source.py.backup
```

#### 4.2 Implement HubSpot Tickets Data Source
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
    Create DLT source function for HubSpot tickets extraction with checkpoint support
    
    Args:
        api_service: HubSpotTicketsAPIService instance
        auth_config: Authentication config from request (contains accessToken)
        filters: Extraction filters and configuration
        checkpoint_callback: Function to save checkpoints during extraction
        resume_from: Resume state from previous checkpoint
    """
    
    # Extract auth details from request
    access_token = auth_config.get('accessToken')
    
    if not access_token:
        raise ValueError("Missing required auth parameter: accessToken")
    
    @dlt.resource(name="ticket_properties", write_disposition="replace", primary_key="name")
    def get_ticket_properties() -> Iterator[Dict[str, Any]]:
        """Extract ticket properties schema"""
        logger = logging.getLogger(__name__)
        
        try:
            logger.info("Starting ticket properties extraction")
            data = api_service.get_ticket_properties(access_token)
            
            properties_count = 0
            for property_def in data.get('results', []):
                # Add extraction metadata
                property_def.update({
                    '_extracted_at': datetime.now(timezone.utc).isoformat(),
                    '_scan_id': filters.get('scan_id'),
                    '_tenant_id': filters.get('organization_id'),
                    '_extraction_type': 'ticket_property'
                })
                yield property_def
                properties_count += 1
            
            logger.info(f"Ticket properties extraction completed. Total properties: {properties_count}")
            
        except Exception as e:
            logger.error(f"Error extracting ticket properties: {e}")
            raise
    
    @dlt.resource(name="tickets", write_disposition="replace", primary_key="id")
    def get_tickets() -> Iterator[Dict[str, Any]]:
        """Extract tickets with their properties and associations"""
        logger = logging.getLogger(__name__)
        job_id = filters.get('scan_id', 'unknown')
        
        # Configure what to extract based on filters
        include_associations = filters.get('include_associations', True)
        associations_to_get = filters.get('associations', ['contacts', 'companies']) if include_associations else []
        
        # Custom properties to extract (in addition to defaults)
        custom_properties = filters.get('properties', [])
        
        # Resume from checkpoint if available
        if resume_from:
            after_cursor = resume_from.get('cursor')
            page_count = resume_from.get('page_number', 0)
            total_records = resume_from.get('records_processed', 0)
            logger.info(f"Resuming tickets extraction from page {page_count + 1}, cursor: {after_cursor}")
        else:
            after_cursor = None
            page_count = 0
            total_records = 0
            logger.info("Starting fresh tickets extraction")
        
        try:
            # Get all available ticket properties first
            logger.info("Getting ticket properties for extraction")
            properties_data = api_service.get_ticket_properties(access_token)
            all_properties = [prop['name'] for prop in properties_data.get('results', [])]
            
            # Combine default properties with custom ones
            properties_to_extract = list(set([
                'hs_ticket_id', 'subject', 'content', 'hs_ticket_category',
                'hs_ticket_priority', 'hs_pipeline', 'hs_pipeline_stage',
                'hubspot_owner_id', 'source_type', 'hs_created_by_user_id',
                'createdate', 'hs_lastmodifieddate', 'closed_date',
                'hs_resolution', 'time_to_close', 'time_to_first_customer_reply'
            ] + custom_properties))
            
            # Filter to only valid properties
            valid_properties = [prop for prop in properties_to_extract if prop in all_properties]
            logger.info(f"Extracting {len(valid_properties)} properties per ticket")
            
            while page_count < 10000:  # Safety limit
                try:
                    logger.debug(f"Fetching tickets page {page_count + 1} (after: {after_cursor})")
                    
                    # Get tickets from HubSpot
                    data = api_service.get_tickets(
                        access_token=access_token,
                        limit=100,  # HubSpot max batch size
                        after=after_cursor,
                        properties=valid_properties,
                        associations=associations_to_get if include_associations else None
                    )
                    
                    tickets = data.get('results', [])
                    if not tickets:
                        logger.info("No more tickets to process")
                        break
                    
                    # Process each ticket
                    for ticket in tickets:
                        ticket_id = ticket.get('id')
                        ticket_properties = ticket.get('properties', {})
                        
                        # Flatten ticket structure
                        flattened_ticket = {
                            'id': ticket_id,
                            'created_at': ticket.get('createdAt'),
                            'updated_at': ticket.get('updatedAt'),
                            **ticket_properties  # Spread all properties
                        }
                        
                        # Get associations if requested
                        if include_associations and ticket_id:
                            associations_data = {}
                            
                            for assoc_type in associations_to_get:
                                try:
                                    assoc_data = api_service.get_ticket_associations(
                                        access_token, ticket_id, assoc_type
                                    )
                                    
                                    association_ids = [
                                        assoc['toObjectId'] for assoc in assoc_data.get('results', [])
                                    ]
                                    
                                    if association_ids:
                                        associations_data[f'associated_{assoc_type}'] = association_ids
                                        
                                        # Get details for first associated record (to avoid too many API calls)
                                        if assoc_type == 'contacts' and association_ids:
                                            contact_details = api_service.get_contact(
                                                access_token, association_ids[0]
                                            )
                                            if contact_details:
                                                contact_props = contact_details.get('properties', {})
                                                associations_data['primary_contact'] = {
                                                    'id': contact_details.get('id'),
                                                    'email': contact_props.get('email'),
                                                    'name': f"{contact_props.get('firstname', '')} {contact_props.get('lastname', '')}".strip()
                                                }
                                        
                                        elif assoc_type == 'companies' and association_ids:
                                            company_details = api_service.get_company(
                                                access_token, association_ids[0]
                                            )
                                            if company_details:
                                                company_props = company_details.get('properties', {})
                                                associations_data['primary_company'] = {
                                                    'id': company_details.get('id'),
                                                    'name': company_props.get('name'),
                                                    'domain': company_props.get('domain')
                                                }
                                
                                except Exception as assoc_error:
                                    logger.warning(f"Could not get {assoc_type} associations for ticket {ticket_id}: {assoc_error}")
                            
                            # Add associations to ticket data
                            flattened_ticket.update(associations_data)
                        
                        # Add extraction metadata
                        flattened_ticket.update({
                            '_extracted_at': datetime.now(timezone.utc).isoformat(),
                            '_scan_id': filters.get('scan_id'),
                            '_tenant_id': filters.get('organization_id'),
                            '_extraction_type': 'ticket',
                            '_page_number': page_count + 1
                        })
                        
                        yield flattened_ticket
                        total_records += 1
                    
                    page_count += 1
                    logger.debug(f"Processed page {page_count}: {len(tickets)} tickets")
                    
                    # Save checkpoint every 10 pages
                    if checkpoint_callback and page_count % 10 == 0:
                        try:
                            next_cursor = None
                            if 'paging' in data and 'next' in data['paging']:
                                next_cursor = data['paging']['next'].get('after')
                            
                            checkpoint_data = {
                                'phase': 'tickets',
                                'records_processed': total_records,
                                'cursor': next_cursor,
                                'page_number': page_count,
                                'batch_size': 100,
                                'checkpoint_data': {
                                    'pages_processed': page_count,
                                    'last_page_tickets': len(tickets),
                                    'extraction_phase': 'tickets_active',
                                    'properties_count': len(valid_properties),
                                    'associations_enabled': include_associations
                                }
                            }
                            
                            checkpoint_callback(job_id, checkpoint_data)
                            logger.info(f"Checkpoint saved at page {page_count}: {total_records} total tickets")
                            
                        except Exception as checkpoint_error:
                            logger.warning(f"Failed to save checkpoint: {checkpoint_error}")
                    
                    # Handle pagination
                    if 'paging' in data and 'next' in data['paging']:
                        after_cursor = data['paging']['next'].get('after')
                        if not after_cursor:
                            logger.info("No pagination cursor found, ending extraction")
                            break
                    else:
                        # Final checkpoint on completion
                        if checkpoint_callback:
                            try:
                                final_checkpoint = {
                                    'phase': 'tickets_completed',
                                    'records_processed': total_records,
                                    'cursor': None,
                                    'page_number': page_count,
                                    'checkpoint_data': {
                                        'completion_status': 'success',
                                        'total_pages': page_count,
                                        'final_total': total_records,
                                        'properties_extracted': len(valid_properties)
                                    }
                                }
                                checkpoint_callback(job_id, final_checkpoint)
                                logger.info(f"Final checkpoint saved: {total_records} total tickets")
                            except Exception as e:
                                logger.warning(f"Failed to save final checkpoint: {e}")
                        
                        logger.info(f"Tickets extraction completed. Total tickets: {total_records}, Pages: {page_count}")
                        break
                        
                except Exception as e:
                    logger.error(f"Error fetching tickets page {page_count + 1}: {str(e)}")
                    
                    # Save error checkpoint for debugging
                    if checkpoint_callback:
                        try:
                            error_checkpoint = {
                                'phase': 'tickets_error',
                                'records_processed': total_records,
                                'cursor': after_cursor,
                                'page_number': page_count,
                                'checkpoint_data': {
                                    'error': str(e),
                                    'error_page': page_count + 1,
                                    'recovery_cursor': after_cursor
                                }
                            }
                            checkpoint_callback(job_id, error_checkpoint)
                        except:
                            pass
                    
                    raise e
                    
        except Exception as e:
            logger.error(f"Error during tickets extraction: {str(e)}")
            raise
    
    return [get_ticket_properties, get_tickets]
```

---

### Step 5: Update Extraction Service

#### 5.1 Edit `services/extraction_service.py`

**Find line ~15:**
```python
from .api_service import APIService
```

**Replace with:**
```python
from .hubspot_tickets_api_service import HubSpotTicketsAPIService
```

**Find around line 30:**
```python
self.api_service = APIService()
```

**Replace with:**
```python
self.api_service = HubSpotTicketsAPIService()
```

**Find around line 35:**
```python
def __init__(self, config: Dict[str, Any], source_type: str = "hubspot_users"):
```

**Replace with:**
```python
def __init__(self, config: Dict[str, Any], source_type: str = "hubspot_tickets"):
```

---

### Step 6: Update API Schemas

#### 6.1 Edit `api/schemas.py`

The existing `AuthSchema` should work for HubSpot, but let's verify it looks correct:

**Ensure this is in `api/schemas.py`:**
```python
class AuthSchema(Schema):
    """Authentication schema"""
    accessToken = fields.Str(
        required=True,
        validate=validate.Length(min=10),
        error_messages={'required': 'Access token is required'}
    )
```

If you want to add custom filters for tickets, add this:

```python
class TicketFiltersSchema(Schema):
    """Ticket-specific filters schema"""
    properties = fields.List(
        fields.Str(),
        allow_none=True,
        validate=validate.Length(min=1),
        error_messages={'validator_failed': 'Properties list cannot be empty'}
    )
    include_associations = fields.Bool(
        missing=True,
        default=True
    )
    associations = fields.List(
        fields.Str(validate=validate.OneOf(['contacts', 'companies', 'deals'])),
        missing=['contacts', 'companies'],
        default=['contacts', 'companies']
    )
    date_range = fields.Nested(DateRangeSchema, allow_none=True)
```

---

## Testing & Validation

### Step 1: Restart Services
```bash
# Stop services
docker-compose down

# Rebuild and start
docker-compose up -d --build

# Check logs for any errors
docker-compose logs -f hubspot_service
```

### Step 2: Test Health Check
```bash
curl http://localhost:5000/health
```

**Expected Response:**
```json
{
  "status": "healthy",
  "timestamp": "2025-01-XX...",
  "service": "hubspot_tickets_extraction",
  "pipeline": {
    "pipeline_name": "hubspot_tickets_extraction",
    "destination_type": "postgres",
    "is_active": true,
    "source_type": "hubspot_tickets"
  }
}
```

### Step 3: Test API Documentation
Open browser: `http://localhost:5000/docs/`

You should see Swagger UI showing the `accessToken` field in auth schema.

### Step 4: Test Credentials Validation
```bash
curl -X POST http://localhost:5000/api/v1/scan/start \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "scanId": "test-hubspot-tickets-001",
      "organizationId": "12345678",
      "type": ["ticket"],
      "auth": {
        "accessToken": "pat-na1-your-hubspot-token-here"
      },
      "filters": {
        "include_associations": true,
        "associations": ["contacts", "companies"],
        "properties": ["hs_ticket_category", "hs_resolution"]
      }
    }
  }'
```

**Expected Success Response:**
```json
{
  "success": true,
  "scanId": "test-hubspot-tickets-001",
  "status": "running",
  "startTime": "2025-01-XX...",
  "message": "hubspot_tickets extraction scan initiated successfully"
}
```

### Step 5: Monitor Scan Progress
```bash
# Check scan status
curl http://localhost:5000/api/v1/scan/test-hubspot-tickets-001/status

# Check detailed logs
docker-compose logs -f hubspot_service
```

### Step 6: Verify Data Extraction
After the scan completes (status: "completed"):

```bash
# Get available tables
curl http://localhost:5000/api/v1/results/test-hubspot-tickets-001/tables

# Get ticket properties schema
curl "http://localhost:5000/api/v1/results/test-hubspot-tickets-001/data?tableName=ticket_properties&limit=10"

# Get tickets data
curl "http://localhost:5000/api/v1/results/test-hubspot-tickets-001/data?tableName=tickets&limit=10"
```

**Expected Response Structure:**
```json
{
  "success": true,
  "data": {
    "scanId": "test-hubspot-tickets-001",
    "tableName": "tickets",
    "records": [
      {
        "id": "1234567890",
        "subject": "Customer support request",
        "hs_ticket_priority": "MEDIUM",
        "hs_pipeline": "0",
        "hs_pipeline_stage": "1",
        "createdate": "2024-12-15T10:30:00.000Z",
        "associated_contacts": ["contact_id_1"],
        "primary_contact": {
          "id": "contact_id_1",
          "email": "customer@example.com",
          "name": "John Doe"
        },
        "_extracted_at": "2025-01-XX...",
        "_tenant_id": "12345678"
      }
    ]
  }
}
```

---

## Production Deployment

### Step 1: Production Environment Setup

#### 1.1 Create Production Environment File
```bash
cp .env .env.production
```

#### 1.2 Edit `.env.production`
```env
# Production Configuration
FLASK_ENV=production
FLASK_DEBUG=false
SECRET_KEY=your-32-character-production-secret-key-here
DB_PASSWORD=secure-production-password

# Service Configuration
DLT_PIPELINE_NAME=hubspot_tickets_extraction
DB_NAME=hubspot_tickets_prod
DB_SCHEMA=hubspot_tickets

# HubSpot API
HUBSPOT_API_BASE_URL=https://api.hubapi.com
HUBSPOT_API_TIMEOUT=30

# Production Optimizations
MAX_CONCURRENT_SCANS=8
DB_POOL_SIZE=25
DB_MAX_OVERFLOW=50
LOG_LEVEL=INFO

# Security
CORS_ORIGINS=https://your-frontend-domain.com
```

### Step 2: Production Deployment

#### 2.1 Deploy with Docker Compose
```bash
# Use production compose file
docker-compose -f docker-compose.prod.yml up -d --build

# Check production health
curl https://your-production-domain.com/health
```

### Step 3: Production Monitoring

#### 3.1 Health Check Monitoring
Set up monitoring for these endpoints:
- `GET /health` - Overall service health
- `GET /api/v1/pipeline/info` - Pipeline status
- `GET /api/v1/scan/statistics` - Job statistics

#### 3.2 Log Monitoring
Monitor these log patterns:
```bash
# Successful extractions
grep "Tickets extraction completed" /var/log/hubspot-service/app.log

# Rate limiting
grep "Rate limited" /var/log/hubspot-service/app.log

# Authentication failures  
grep "Credential validation failed" /var/log/hubspot-service/app.log

# Errors
grep "ERROR" /var/log/hubspot-service/app.log
```

---

## Troubleshooting

### Common Issues and Solutions

#### 1. Authentication Errors

**Issue**: `"Invalid hubspot_tickets access credentials"`

**Causes & Solutions:**
```bash
# Check if private app has correct scopes
# Required: tickets (Read), crm.objects.contacts.read, crm.objects.companies.read

# Verify the private app is active
# In HubSpot: Settings > Integrations > Private Apps > Your App > Status

# Test credentials manually
curl -H "Authorization: Bearer your-token" \
     https://api.hubapi.com/crm/v3/properties/tickets
```

#### 2. Rate Limiting Issues

**Issue**: Frequent rate limiting from HubSpot API

**Solutions:**
```python
# Reduce batch size in data_source.py
data = api_service.get_tickets(
    access_token=access_token,
    limit=50,  # Reduced from 100
    after=after_cursor
)

# Increase checkpoint frequency  
if checkpoint_callback and page_count % 5 == 0:  # Every 5 pages instead of 10

# Add delay between association requests
import time
time.sleep(0.2)  # 200ms delay between association API calls
```

#### 3. Missing Ticket Properties

**Issue**: Some ticket properties not being extracted

**Solutions:**
```bash
# Check available properties
curl "http://localhost:5000/api/v1/results/your-scan-id/data?tableName=ticket_properties&limit=100"

# Add custom properties to your request
{
  "filters": {
    "properties": ["custom_property_1", "custom_property_2"],
    "include_associations": true
  }
}

# Verify property names in HubSpot
# Go to: Settings > Properties > Ticket properties
```

#### 4. Database Connection Issues

**Issue**: `"Database connection failed"`

**Solutions:**
```bash
# Check database container
docker-compose ps
docker-compose logs postgres

# Test direct connection
docker exec -it hubspot_postgres pg_isready -U postgres -d hubspot_tickets_data

# Verify schema exists
docker exec -it hubspot_postgres psql -U postgres -d hubspot_tickets_data -c "\dn"
```

#### 5. No Associations Retrieved

**Issue**: Tickets extracted but no contact/company associations

**Solutions:**
```python
# Enable associations in request
{
  "filters": {
    "include_associations": true,
    "associations": ["contacts", "companies", "deals"]
  }
}

# Check if private app has association permissions
# Required scopes: crm.objects.contacts.read, crm.objects.companies.read

# Test associations manually
curl -H "Authorization: Bearer your-token" \
     https://api.hubapi.com/crm/v4/objects/tickets/TICKET_ID/associations/contacts
```

#### 6. Memory Issues with Large Portals

**Issue**: Service runs out of memory with large HubSpot portals

**Solutions:**
```python
# Reduce association details fetching
# In data_source.py, comment out detailed contact/company fetching
# Just keep the association IDs

# Process in smaller batches
data = api_service.get_tickets(
    access_token=access_token,
    limit=25,  # Smaller batches
    after=after_cursor
)

# Increase checkpoint frequency
checkpoint_interval = 3  # Save every 3 pages
```

### Debugging Steps

#### 1. Enable Debug Logging
```bash
# Set environment variable
export LOG_LEVEL=DEBUG

# Restart service
docker-compose restart hubspot_service

# Monitor detailed logs
docker-compose logs -f hubspot_service
```

#### 2. Test Individual Components

**Test Authentication:**
```bash
curl -X POST http://localhost:5000/api/v1/scan/start \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "scanId": "auth-test-001", 
      "organizationId": "12345678",
      "type": ["ticket"],
      "auth": {
        "accessToken": "pat-na1-your-token-here"
      }
    }
  }'
```

**Test HubSpot API Directly:**
```bash
# Test tickets endpoint
curl -H "Authorization: Bearer pat-na1-your-token" \
     "https://api.hubapi.com/crm/v3/objects/tickets?limit=1"

# Test properties endpoint  
curl -H "Authorization: Bearer pat-na1-your-token" \
     "https://api.hubapi.com/crm/v3/properties/tickets"
```

#### 3. Database Inspection

**Connect to Database:**
```bash
docker exec -it hubspot_postgres psql -U postgres -d hubspot_tickets_data
```

**Check Extracted Data:**
```sql
-- List all schemas
\dn

-- Connect to tenant schema
SET search_path TO hubspot_tickets_12345678;

-- Check tables
\dt

-- Check data
SELECT COUNT(*) FROM ticket_properties;
SELECT COUNT(*) FROM tickets; 
SELECT subject, hs_ticket_priority, createdate FROM tickets LIMIT 5;
```

---

## API Usage Examples

### Basic Ticket Extraction
```bash
curl -X POST http://localhost:5000/api/v1/scan/start \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "scanId": "hubspot-tickets-portal123-2025-01",
      "organizationId": "12345678",
      "type": ["ticket"],
      "auth": {
        "accessToken": "pat-na1-your-hubspot-private-app-token"
      },
      "filters": {
        "include_associations": true,
        "associations": ["contacts", "companies"]
      }
    }
  }'
```

### Advanced Ticket Extraction with Custom Properties
```bash
curl -X POST http://localhost:5000/api/v1/scan/start \
  -H "Content-Type: application/json" \
  -d '{
    "config": {
      "scanId": "hubspot-tickets-full-portal123-2025-01", 
      "organizationId": "12345678",
      "type": ["ticket"],
      "auth": {
        "accessToken": "pat-na1-your-token"
      },
      "filters": {
        "properties": [
          "custom_ticket_category",
          "sla_deadline",
          "customer_satisfaction_score",
          "resolution_notes"
        ],
        "include_associations": true,
        "associations": ["contacts", "companies", "deals"]
      }
    }
  }'
```

### Check Extraction Results
```bash
# Get scan status
curl http://localhost:5000/api/v1/scan/hubspot-tickets-portal123-2025-01/status

# Get available tables 
curl http://localhost:5000/api/v1/results/hubspot-tickets-portal123-2025-01/tables

# Get ticket properties
curl "http://localhost:5000/api/v1/results/hubspot-tickets-portal123-2025-01/data?tableName=ticket_properties&limit=50"

# Get tickets with pagination
curl "http://localhost:5000/api/v1/results/hubspot-tickets-portal123-2025-01/data?tableName=tickets&limit=100&offset=0"

# Get recent tickets
curl "http://localhost:5000/api/v1/results/hubspot-tickets-portal123-2025-01/data?tableName=tickets&limit=10" | jq '.data.records[] | {id, subject, priority: .hs_ticket_priority, created: .createdate}'
```

---

## Performance Optimization

### For Large HubSpot Portals (10,000+ Tickets)

#### 1. Batch Configuration
```python
# In data_source.py, reduce batch size for large portals
data = api_service.get_tickets(
    access_token=access_token,
    limit=50,  # Reduced from 100
    after=after_cursor
)

# Increase checkpoint frequency
if checkpoint_callback and page_count % 5 == 0:  # Every 5 pages instead of 10
```

#### 2. Resource Allocation
```yaml
# In docker-compose.prod.yml
services:
  hubspot_service:
    deploy:
      resources:
        limits:
          memory: 6G      # Increased memory for large portals
          cpus: '3.0'     # More CPU cores
```

#### 3. Database Optimization
```env
# Increase connection pool for high throughput
DB_POOL_SIZE=25
DB_MAX_OVERFLOW=50

# Longer timeouts for large batches
DB_POOL_TIMEOUT=60
```

### For Frequent Incremental Updates

#### 1. Date-Based Filtering
```python
# Add lastmodifieddate filter for incremental updates
def get_recent_tickets(self, access_token: str, since_date: str = None):
    params = {
        'limit': 100,
        'properties': self.default_properties
    }
    
    if since_date:
        # Use HubSpot search API for date filtering
        url = f"{self.base_url}/crm/v3/objects/tickets/search"
        search_body = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GT",
                    "value": since_date
                }]
            }],
            "properties": self.default_properties,
            "limit": 100
        }
        return self.session.post(url, json=search_body)
```

---

## Next Steps

### 1. ✅ Complete Implementation
Follow all steps in this guide exactly - don't skip any steps.

### 2. 🧪 Test with Sample Portal
Start with a HubSpot portal that has a small number of tickets to validate the implementation.

### 3. 📊 Monitor Performance  
Watch memory usage, API rate limits, and extraction speed during initial runs.

### 4. 🔧 Customize for Your Needs
- Add additional custom ticket properties
- Include more association types (deals, tasks, etc.)
- Implement incremental extraction for regular syncing
- Add custom filters based on ticket pipelines or categories

### 5. 🚀 Production Deployment