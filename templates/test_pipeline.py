"""
Unit tests for HubSpot Deals Pipeline
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from hubspot_deals_pipeline import HubSpotDealsSource, hubspot_deals_source


class TestHubSpotDealsSource:
    """Test HubSpotDealsSource class"""
    
    def test_initialization(self):
        """Test source initialization"""
        api_key = "test_api_key"
        source = HubSpotDealsSource(api_key=api_key)
        
        assert source.api_key == api_key
        assert source.base_url == "https://api.hubapi.com"
        assert source.headers["Authorization"] == f"Bearer {api_key}"
    
    @patch('hubspot_deals_pipeline.requests.get')
    def test_deals_single_page(self, mock_get):
        """Test deals extraction with single page"""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "id": "123",
                    "properties": {
                        "dealname": "Test Deal",
                        "amount": "10000"
                    },
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z",
                    "archived": False
                }
            ]
        }
        mock_get.return_value = mock_response
        
        # Create source and fetch deals
        source = HubSpotDealsSource(api_key="test_key")
        deals = list(source.deals(limit=10))
        
        # Assertions
        assert len(deals) == 1
        assert deals[0]["id"] == "123"
        assert deals[0]["dealname"] == "Test Deal"
        assert deals[0]["amount"] == "10000"
    
    @patch('hubspot_deals_pipeline.requests.get')
    def test_deals_pagination(self, mock_get):
        """Test deals extraction with pagination"""
        # Mock first page
        first_response = Mock()
        first_response.status_code = 200
        first_response.json.return_value = {
            "results": [
                {
                    "id": "123",
                    "properties": {"dealname": "Deal 1"},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z",
                    "archived": False
                }
            ],
            "paging": {
                "next": {
                    "after": "cursor123"
                }
            }
        }
        
        # Mock second page
        second_response = Mock()
        second_response.status_code = 200
        second_response.json.return_value = {
            "results": [
                {
                    "id": "456",
                    "properties": {"dealname": "Deal 2"},
                    "createdAt": "2024-01-03T00:00:00Z",
                    "updatedAt": "2024-01-04T00:00:00Z",
                    "archived": False
                }
            ]
        }
        
        # Set up mock to return different responses
        mock_get.side_effect = [first_response, second_response]
        
        # Create source and fetch deals
        source = HubSpotDealsSource(api_key="test_key")
        deals = list(source.deals(limit=10))
        
        # Assertions
        assert len(deals) == 2
        assert deals[0]["id"] == "123"
        assert deals[1]["id"] == "456"
        assert mock_get.call_count == 2
    
    @patch('hubspot_deals_pipeline.requests.get')
    @patch('hubspot_deals_pipeline.time.sleep')
    def test_rate_limiting(self, mock_sleep, mock_get):
        """Test rate limiting handling"""
        # Mock rate limited response first
        rate_limit_response = Mock()
        rate_limit_response.status_code = 429
        rate_limit_response.headers = {'Retry-After': '5'}
        
        # Mock successful response after retry
        success_response = Mock()
        success_response.status_code = 200
        success_response.json.return_value = {
            "results": [
                {
                    "id": "123",
                    "properties": {"dealname": "Test Deal"},
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z",
                    "archived": False
                }
            ]
        }
        
        # Set up mock to return rate limit then success
        mock_get.side_effect = [rate_limit_response, success_response]
        
        # Create source and fetch deals
        source = HubSpotDealsSource(api_key="test_key")
        deals = list(source.deals(limit=10))
        
        # Assertions
        assert len(deals) == 1
        assert mock_sleep.called
        assert mock_sleep.call_args[0][0] == 5  # Should sleep for 5 seconds
    
    @patch('hubspot_deals_pipeline.requests.get')
    def test_custom_properties(self, mock_get):
        """Test requesting custom properties"""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "id": "123",
                    "properties": {
                        "dealname": "Test Deal",
                        "amount": "10000",
                        "custom_field": "custom_value"
                    },
                    "createdAt": "2024-01-01T00:00:00Z",
                    "updatedAt": "2024-01-02T00:00:00Z",
                    "archived": False
                }
            ]
        }
        mock_get.return_value = mock_response
        
        # Create source and fetch deals with custom properties
        source = HubSpotDealsSource(api_key="test_key")
        custom_props = ["dealname", "amount", "custom_field"]
        deals = list(source.deals(limit=10, properties=custom_props))
        
        # Verify properties parameter was passed
        call_args = mock_get.call_args
        assert "properties" in call_args[1]["params"]
        assert call_args[1]["params"]["properties"] == ",".join(custom_props)


class TestPipelineFunctions:
    """Test pipeline helper functions"""
    
    @patch('hubspot_deals_pipeline.HubSpotDealsSource')
    def test_hubspot_deals_source(self, mock_source_class):
        """Test hubspot_deals_source function"""
        # Mock the source
        mock_source = Mock()
        mock_source_class.return_value = mock_source
        
        # Call the function
        api_key = "test_key"
        result = hubspot_deals_source(api_key=api_key, limit=50)
        
        # Assertions
        mock_source_class.assert_called_once_with(api_key=api_key)
        mock_source.deals.assert_called_once()


class TestErrorHandling:
    """Test error handling scenarios"""
    
    @patch('hubspot_deals_pipeline.requests.get')
    def test_http_error_handling(self, mock_get):
        """Test HTTP error handling"""
        # Mock HTTP error
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server Error")
        mock_get.return_value = mock_response
        
        # Create source and attempt to fetch deals
        source = HubSpotDealsSource(api_key="test_key")
        
        with pytest.raises(Exception):
            list(source.deals(limit=10))
    
    @patch('hubspot_deals_pipeline.requests.get')
    def test_invalid_json_handling(self, mock_get):
        """Test invalid JSON response handling"""
        # Mock response with invalid JSON
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response
        
        # Create source and attempt to fetch deals
        source = HubSpotDealsSource(api_key="test_key")
        
        with pytest.raises(ValueError):
            list(source.deals(limit=10))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])