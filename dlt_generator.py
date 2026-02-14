#!/usr/bin/env python3
"""
DLT Generator
A robust command-line tool to copy everything from a template folder with a new name
and replace placeholders with custom values from config.json.
"""

import os
import shutil
import argparse
import sys
import re
import logging
import json
from pathlib import Path
from typing import Dict, Optional, Any


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from JSON file.
    
    Args:
        config_path (str): Path to the config.json file
        
    Returns:
        dict: Configuration dictionary
        
    Raises:
        ValueError: If config file is invalid or missing required fields
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        # Create a sample config file
        sample_config = {
            "project_name": "my-dlt-service",
            "service_name": "example",
            "template_path": "./template",
            "destination_dir": "./",
            "ports": {
                "dev": 5000,
                "stage": 5001,
                "prod": 5002
            },
            "force_overwrite": False,
            "verbose": False
        }
        
        try:
            config_file.write_text(json.dumps(sample_config, indent=2))
            logger.info(f"Created sample config file: {config_path}")
            logger.info("Please edit the config.json file and run the command again.")
            sys.exit(0)
        except Exception as e:
            raise ValueError(f"Could not create sample config file: {e}")
    
    try:
        config_content = config_file.read_text(encoding='utf-8')
        config = json.loads(config_content)
        
        # Validate required fields
        required_fields = ["project_name", "service_name"]
        missing_fields = [field for field in required_fields if not config.get(field)]
        
        if missing_fields:
            raise ValueError(f"Missing required fields in config.json: {missing_fields}")
        
        # Set defaults for optional fields
        config.setdefault("template_path", "./template")
        config.setdefault("destination_dir", "./")
        config.setdefault("force_overwrite", False)
        config.setdefault("verbose", False)
        config.setdefault("ports", {})
        
        # Validate ports if provided
        if config["ports"]:
            for env, port in config["ports"].items():
                if not isinstance(port, int) or port < 1024 or port > 65535:
                    raise ValueError(f"Invalid port for {env}: {port}. Must be integer between 1024-65535.")
        
        return config
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in config file: {e}")
    except Exception as e:
        raise ValueError(f"Error reading config file: {e}")


def is_binary_file(file_path: Path) -> bool:
    """
    Check if a file is binary by examining its content.
    
    Args:
        file_path (Path): Path to the file
        
    Returns:
        bool: True if file appears to be binary
    """
    try:
        # Check file extension first (faster)
        binary_extensions = {
            '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.ico', '.tiff', '.webp',
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.tar', '.gz', '.bz2', '.xz', '.7z', '.rar',
            '.exe', '.dll', '.so', '.dylib', '.app',
            '.pyc', '.pyo', '.egg', '.whl',
            '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv',
            '.db', '.sqlite', '.sqlite3'
        }
        
        if file_path.suffix.lower() in binary_extensions:
            return True
        
        # Read a small chunk to detect binary content
        try:
            with open(file_path, 'rb') as f:
                chunk = f.read(1024)
                if not chunk:
                    return False
                # Check for null bytes (common in binary files)
                return b'\0' in chunk
        except (IOError, OSError):
            return True
            
    except Exception:
        return True


def replace_placeholders_in_file(file_path: Path, replacements: Dict[str, str]) -> bool:
    """
    Replace placeholders in a single file.
    
    Args:
        file_path (Path): Path to the file
        replacements (dict): Dictionary of placeholder -> replacement mappings
        
    Returns:
        bool: True if file was modified, False otherwise
    """
    try:
        # Skip binary files
        if is_binary_file(file_path):
            logger.debug(f"Skipping binary file: {file_path}")
            return False
        
        # Read file content
        try:
            content = file_path.read_text(encoding='utf-8')
        except UnicodeDecodeError:
            try:
                content = file_path.read_text(encoding='latin-1')
            except UnicodeDecodeError:
                logger.warning(f"Could not decode file as text: {file_path}")
                return False
        
        original_content = content
        
        # Replace all placeholders
        for placeholder, replacement in replacements.items():
            if placeholder in content:
                content = content.replace(placeholder, replacement)
                logger.debug(f"Replaced '{placeholder}' with '{replacement}' in {file_path}")
        
        # Write back if content was modified
        if content != original_content:
            file_path.write_text(content, encoding='utf-8')
            return True
            
        return False
        
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return False


def generate_port_assignments(service_name: str, ports_config: Dict[str, int] = None) -> Dict[str, int]:
    """
    Generate port assignments for dev, stage, and prod environments.
    
    Args:
        service_name (str): Service name for consistent port assignment
        ports_config (dict): Port configuration from config.json
        
    Returns:
        dict: Dictionary with 'dev', 'stage', 'prod' port assignments
    """
    # If ports are provided in config, use them
    if ports_config:
        default_ports = {
            'dev': ports_config.get('dev', 5000),
            'stage': ports_config.get('stage', 5001),
            'prod': ports_config.get('prod', 5002)
        }
        return default_ports
    
    # Generate consistent ports from service name hash
    service_hash = abs(hash(service_name.lower())) % 1000
    base_port = 5000 + service_hash
    
    # Ensure we don't conflict with common ports
    while base_port in [5432, 6379, 8080, 8081, 9000]:
        base_port += 1
    
    # Validate base port range
    if base_port < 3000 or base_port > 65000:
        base_port = 5000
    
    return {
        'dev': base_port,
        'stage': base_port + 1,
        'prod': base_port + 2
    }


def generate_replacements(service_name: str, ports: Dict[str, int] = None) -> Dict[str, str]:
    """
    Generate replacement mappings based on the service name and ports.
    
    Args:
        service_name (str): Name of the service (e.g., "salesforce", "stripe")
        ports (dict): Dictionary with 'dev', 'stage', 'prod' port numbers
    
    Returns:
        dict: Dictionary of placeholder -> replacement mappings
    """
    # Sanitize service name
    service_name = service_name.strip()
    if not service_name:
        raise ValueError("Service name cannot be empty")
    
    # Convert service name variations
    service_lower = service_name.lower()
    service_upper = service_name.upper()
    service_title = service_name.title()
    
    # Create snake_case (replace spaces and hyphens with underscores)
    service_snake = re.sub(r'[-\s]+', '_', service_lower)
    service_snake = re.sub(r'[^a-z0-9_]', '', service_snake)
    
    # Create kebab-case (replace spaces and underscores with hyphens)
    service_kebab = re.sub(r'[_\s]+', '-', service_lower)
    service_kebab = re.sub(r'[^a-z0-9-]', '', service_kebab)
    
    replacements = {
        '{{SERVICE_NAME}}': service_name,
        '{{SERVICE_NAME_LOWER}}': service_lower,
        '{{SERVICE_NAME_UPPER}}': service_upper,
        '{{SERVICE_NAME_TITLE}}': service_title,
        '{{SERVICE_NAME_SNAKE}}': service_snake,
        '{{SERVICE_NAME_KEBAB}}': service_kebab,
    }
    
    # Add port placeholders if ports are provided
    if ports:
        replacements.update({
            '{{PORT_DEV}}': str(ports.get('dev', 5000)),
            '{{PORT_STAGE}}': str(ports.get('stage', 5001)),
            '{{PORT_PROD}}': str(ports.get('prod', 5002)),
        })
    
    return replacements


def validate_paths(template_path: str, destination_name: str, destination_dir: Optional[str] = None) -> tuple:
    """
    Validate and resolve all paths.
    
    Args:
        template_path (str): Path to template folder
        destination_name (str): Name for the new folder
        destination_dir (str, optional): Destination directory
        
    Returns:
        tuple: (template_path, destination_path)
        
    Raises:
        ValueError: If paths are invalid
    """
    # Validate and resolve template path
    template_path = Path(template_path).resolve()
    if not template_path.exists():
        raise ValueError(f"Template folder '{template_path}' does not exist")
    if not template_path.is_dir():
        raise ValueError(f"Template path '{template_path}' is not a directory")
    
    # Validate destination name
    if not destination_name or not destination_name.strip():
        raise ValueError("Destination name cannot be empty")
    
    # Sanitize destination name
    destination_name = re.sub(r'[<>:"/\\|?*]', '_', destination_name.strip())
    
    # Determine destination directory
    if destination_dir:
        dest_dir = Path(destination_dir).resolve()
    else:
        dest_dir = Path.cwd()
    
    destination_path = dest_dir / destination_name
    
    return template_path, destination_path


def copy_template(config: Dict[str, Any]) -> bool:
    """
    Copy template folder to a new location with a different name and replace placeholders.
    
    Args:
        config (dict): Configuration dictionary from config.json
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Extract configuration
        template_path = config["template_path"]
        destination_name = config["project_name"]
        destination_dir = config.get("destination_dir")
        service_name = config["service_name"]
        force = config.get("force_overwrite", False)
        
        # Validate paths
        template_path, destination_path = validate_paths(
            template_path, destination_name, destination_dir
        )
        
        logger.info(f"Template: {template_path}")
        logger.info(f"Destination: {destination_path}")
        
        # Handle existing destination
        if destination_path.exists():
            if not force:
                response = input(f"Destination '{destination_path}' already exists. Overwrite? (y/N): ")
                if response.lower() not in ['y', 'yes']:
                    logger.info("Operation cancelled by user")
                    return False
            
            logger.info(f"Removing existing destination: {destination_path}")
            if destination_path.is_dir():
                shutil.rmtree(destination_path)
            else:
                destination_path.unlink()
        
        # Create destination directory if needed
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy the template folder
        logger.info("Copying template files...")
        shutil.copytree(template_path, destination_path)
        
        # Count files for progress tracking
        all_files = list(destination_path.rglob('*'))
        total_files = sum(1 for f in all_files if f.is_file())
        
        logger.info(f"Copied {total_files} files and {sum(1 for f in all_files if f.is_dir())} directories")
        
        # Replace placeholders
        if service_name:
            logger.info(f"Replacing placeholders with service name '{service_name}'...")
            
            try:
                # Generate port assignments
                ports = generate_port_assignments(service_name, config.get("ports"))
                logger.info(f"Port assignments - Dev: {ports['dev']}, Stage: {ports['stage']}, Prod: {ports['prod']}")
                
                replacements = generate_replacements(service_name, ports)
                logger.debug(f"Generated replacements: {replacements}")
                
                processed_files = 0
                modified_files = 0
                
                for file_path in destination_path.rglob('*'):
                    if file_path.is_file():
                        if replace_placeholders_in_file(file_path, replacements):
                            modified_files += 1
                        processed_files += 1
                
                logger.info(f"Processed {processed_files} files, modified {modified_files} files")
                
            except Exception as e:
                logger.error(f"Error during placeholder replacement: {e}")
                return False
        
        logger.info(f"✅ Successfully created DLT service at '{destination_path}'")
        return True
        
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return False
    except PermissionError as e:
        logger.error(f"Permission denied: {e}")
        return False
    except OSError as e:
        logger.error(f"File system error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False


def main():
    """Main entry point for the DLT Generator CLI."""
    parser = argparse.ArgumentParser(
        description="Generate DLT extraction services from templates using config.json",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Configuration File (config.json):
{
  "project_name": "my-salesforce-service",
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

Examples:
  # Use default config.json
  python dlt_generator.py
  
  # Use custom config file
  python dlt_generator.py -c my-config.json
        """
    )
    
    parser.add_argument(
        "-c", "--config",
        default="config.json",
        help="Path to configuration file (default: config.json)"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="DLT Generator 2.0.0"
    )
    
    args = parser.parse_args()
    
    # Load configuration
    try:
        config = load_config(args.config)
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        sys.exit(1)
    
    # Set logging level from config
    if config.get("verbose"):
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose mode enabled")
    
    # Display configuration
    logger.info("=== DLT Generator Configuration ===")
    logger.info(f"Config file: {args.config}")
    logger.info(f"Project name: {config['project_name']}")
    logger.info(f"Service name: {config['service_name']}")
    logger.info(f"Template path: {config['template_path']}")
    logger.info(f"Destination directory: {config['destination_dir']}")
    
    if config.get("ports"):
        ports = config["ports"]
        logger.info(f"Port assignments - Dev: {ports.get('dev', 'auto')}, Stage: {ports.get('stage', 'auto')}, Prod: {ports.get('prod', 'auto')}")
    else:
        sample_ports = generate_port_assignments(config['service_name'])
        logger.info(f"Port assignments (auto-generated) - Dev: {sample_ports['dev']}, Stage: {sample_ports['stage']}, Prod: {sample_ports['prod']}")
    
    logger.info(f"Force overwrite: {config.get('force_overwrite', False)}")
    logger.info("=" * 35)
    
    # Perform the copy operation
    success = copy_template(config)
    
    if not success:
        logger.error("❌ DLT service generation failed")
        sys.exit(1)
    
    logger.info("🎉 DLT service generation completed successfully!")


if __name__ == "__main__":
    main()