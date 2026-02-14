"""
HubSpot API Service
Handles all interactions with HubSpot CRM API v3
"""
import requests
import time
import logging
from typing import Dict, List, Optional, Iterator, Any
from datetime import datetime, timezone
from decimal import Decimal


logger = logging.getLogger(__name__)


class HubSpotAPIError(Exception):
    """Base exception for HubSpot API errors"""
    pass


class HubSpotAuthenticationError(HubSpotAPIError):
    """Raised when authentication fails"""
    pass


class HubSpotRateLimitError(HubSpotAPIError):
    """Raised when rate limit is exceeded"""
    pass


class HubSpot APIService:
    """Service for interacting with HubSpot CRM API v3"""
    
    # Rate Limits (HubSpot: 150 requests per 10 seconds for Professional tier)
    RATE_LIMIT_MAX = 150
    RATE_LIMIT_WINDOW = 10  # seconds
    
    def __init__(self, api_key: str, base_url: str = "https://api.hubapi.com"):
        """
        Initialize HubSpot API Service
        
        Args:
            api_key: HubSpot Private App access token
            base_url: HubSpot API base URL
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        # Rate limiting tracking
        self._request_times: List[float] = []
        
        logger.info("HubSpot API Service initialized")
    
    def validate_credentials(self) -> Dict[str, Any]:
        """
        Validate HubSpot API credentials
        
        Returns:
            Dictionary with validation results including portal_id and scopes
            
        Raises:
            HubSpotAuthenticationError: If credentials are invalid
        """
        logger.info("Validating HubSpot credentials")
        
        try:
            # Test API key by fetching account info
            url = f"{self.base_url}/crm/v3/objects/deals"
            params = {"limit": 1}
            
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=10
            )
            
            if response.status_code == 401:
                raise HubSpotAuthenticationError("Invalid API key")
            
            if response.status_code == 403:
                error_data = response.json()
                raise HubSpotAuthenticationError(
                    f"Insufficient permissions: {error_data.get('message')}"
                )
            
            response.raise_for_status()
            
            # Extract rate limit info from headers
            rate_limit_info = {
                "remaining": int(response.headers.get("X-HubSpot-RateLimit-Remaining", 0)),
                "max": int(response.headers.get("X-HubSpot-RateLimit-Max", 150)),
                "reset_at": None  # Can calculate from interval if needed
            }
            
            logger.info("Credentials validated successfully")
            
            return {
                "valid": True,
                "scopes": ["crm.objects.deals.read"],  # Inferred from successful call
                "rate_limit": rate_limit_info
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Credential validation failed: {e}")
            raise HubSpotAuthenticationError(f"API validation failed: {e}")
    
    def _wait_for_rate_limit(self):
        """
        Implement rate limiting by waiting if necessary
        Ensures we don't exceed HubSpot's rate limits
        """
        now = time.time()
        
        # Remove requests older than the rate limit window
        cutoff_time = now - self.RATE_LIMIT_WINDOW
        self._request_times = [t for t in self._request_times if t > cutoff_time]
        
        # If we're at the limit, wait until the oldest request expires
        if len(self._request_times) >= self.RATE_LIMIT_MAX:
            wait_time = self.RATE_LIMIT_WINDOW - (now - self._request_times[0])
            if wait_time > 0:
                logger.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
                # Clean up old requests again after waiting
                now = time.time()
                cutoff_time = now - self.RATE_LIMIT_WINDOW
                self._request_times = [t for t in self._request_times if t > cutoff_time]
        
        # Record this request
        self._request_times.append(now)
    
    def get_deals(
        self,
        limit: int = 100,
        after: Optional[str] = None,
        properties: Optional[List[str]] = None,
        archived: bool = False,
        timeout: int = 30
    ) -> Dict[str, Any]:
        """
        Fetch deals from HubSpot with pagination support
        
        Args:
            limit: Number of results per page (max 100)
            after: Pagination cursor
            properties: List of deal properties to fetch
            archived: Whether to include archived deals
            timeout: Request timeout in seconds
            
        Returns:
            Dictionary containing results and pagination info
            
        Raises:
            HubSpotAPIError: If API request fails
            HubSpotRateLimitError: If rate limited
        """
        # Apply rate limiting
        self._wait_for_rate_limit()
        
        url = f"{self.base_url}/crm/v3/objects/deals"
        
        # Default properties if none specified
        if properties is None:
            properties = [
                "dealname", "amount", "dealstage", "pipeline", "closedate",
                "createdate", "hs_lastmodifieddate", "hubspot_owner_id",
                "description", "dealtype", "hs_priority",
                "num_associated_contacts", "num_associated_companies",
                "hs_is_closed", "hs_is_closed_won",
                "hs_forecast_amount", "hs_forecast_probability"
            ]
        
        params = {
            "limit": min(limit, 100),  # API max is 100
            "properties": ",".join(properties),
            "archived": str(archived).lower()
        }
        
        if after:
            params["after"] = after
        
        try:
            response = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=timeout
            )
            
            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                logger.warning(f"Rate limited. Retry after {retry_after} seconds")
                raise HubSpotRateLimitError(f"Rate limited. Retry after {retry_after}s")
            
            # Handle authentication errors
            if response.status_code == 401:
                raise HubSpotAuthenticationError("Invalid or expired API key")
            
            # Handle other errors
            response.raise_for_status()
            
            data = response.json()
            
            logger.info(f"Fetched {len(data.get('results', []))} deals")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timeout after {timeout} seconds")
            raise HubSpotAPIError(f"Request timeout after {timeout} seconds")
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise HubSpotAPIError(f"API request failed: {e}")
    
    def get_all_deals(
        self,
        properties: Optional[List[str]] = None,
        archived: bool = False,
        checkpoint_callback: Optional[callable] = None,
        checkpoint_interval: int = 5
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch all deals with automatic pagination and checkpoint support
        
        Args:
            properties: List of deal properties to fetch
            archived: Whether to include archived deals
            checkpoint_callback: Function to call at each checkpoint
            checkpoint_interval: Number of pages between checkpoints
            
        Yields:
            Individual deal records
        """
        after = None
        page_count = 0
        total_deals = 0
        
        while True:
            try:
                data = self.get_deals(
                    limit=100,
                    after=after,
                    properties=properties,
                    archived=archived
                )
                
                results = data.get("results", [])
                page_count += 1
                
                # Yield each deal
                for deal in results:
                    total_deals += 1
                    yield deal
                
                logger.info(f"Page {page_count}: {len(results)} deals (Total: {total_deals})")
                
                # Checkpoint
                if checkpoint_callback and page_count % checkpoint_interval == 0:
                    checkpoint_callback(page=page_count, deals_so_far=total_deals)
                
                # Check for next page
                paging = data.get("paging", {})
                if "next" not in paging:
                    logger.info(f"Extraction complete. Total: {total_deals} deals, {page_count} pages")
                    break
                
                after = paging["next"]["after"]
                
            except HubSpotRateLimitError as e:
                logger.warning(f"Rate limited: {e}. Waiting before retry...")
                time.sleep(10)
                continue
            
            except HubSpotAPIError as e:
                logger.error(f"API error on page {page_count}: {e}")
                raise
    
    def get_rate_limit_status(self) -> Dict[str, int]:
        """
        Get current rate limit status
        
        Returns:
            Dictionary with rate limit information
        """
        now = time.time()
        cutoff_time = now - self.RATE_LIMIT_WINDOW
        active_requests = [t for t in self._request_times if t > cutoff_time]
        
        return {
            "requests_in_window": len(active_requests),
            "max_requests": self.RATE_LIMIT_MAX,
            "window_seconds": self.RATE_LIMIT_WINDOW,
            "remaining": self.RATE_LIMIT_MAX - len(active_requests)
        }


# Example usage and testing
if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Initialize service
    api_key = os.getenv("HUBSPOT_API_KEY")
    if not api_key:
        print("Error: HUBSPOT_API_KEY not set in environment")
        exit(1)
    
    service = HubSpotAPIService(api_key=api_key)
    
    # Validate credentials
    try:
        validation = service.validate_credentials()
        print(f"Credentials valid: {validation}")
    except HubSpotAuthenticationError as e:
        print(f"Authentication failed: {e}")
        exit(1)
    
    # Fetch first page of deals
    try:
        deals_data = service.get_deals(limit=5)
        print(f"\nFetched {len(deals_data['results'])} deals")
        
        for deal in deals_data['results']:
            print(f"- {deal['properties'].get('dealname')}: ${deal['properties'].get('amount', 0)}")
    
    except HubSpotAPIError as e:
        print(f"Error fetching deals: {e}")