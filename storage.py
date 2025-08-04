# storage.py - Enhanced File Storage Operations
"""
The guardian of file persistence - orchestrating the elegant dance between memory and disk,
ensuring data flows seamlessly between processing stages while maintaining integrity
across the application lifecycle with cross-platform compatibility.
"""

from pathlib import Path
from io import BytesIO
import os
import json
import pandas as pd
import logging
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
import shutil

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define storage paths with cross-platform compatibility
BASE_DIR = Path(__file__).parent / "output_data"
STORAGE_PATH = BASE_DIR / "output.csv"
BACKUP_DIR = BASE_DIR / "backups"
TEMP_DIR = BASE_DIR / "temp"

def initialize_storage_directories():
    """
    Initialize storage directory structure with elegant error handling
    """
    try:
        directories = [BASE_DIR, BACKUP_DIR, TEMP_DIR]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
        
        logger.info("Storage directories initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize storage directories: {str(e)}")
        return False

def save_output_to_disk(data: BytesIO, filename: str = "output.csv") -> bool:
    """
    Save BytesIO content to disk with intelligent file management
    """
    try:
        # Ensure storage directory exists
        if not initialize_storage_directories():
            return False
        
        # Determine full file path
        file_path = BASE_DIR / filename
        
        # Reset BytesIO pointer to beginning
        data.seek(0)
        
        # Write data to file with atomic operation
        temp_path = TEMP_DIR / f"temp_{filename}"
        
        with open(temp_path, "wb") as temp_file:
            temp_file.write(data.read())
        
        # Atomic move to final location
        shutil.move(str(temp_path), str(file_path))
        
        logger.info(f"Successfully saved output to: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving output to disk: {str(e)}")
        return False

def load_output_from_disk(filename: str = "output.csv") -> Optional[BytesIO]:
    """
    Load output file from disk as BytesIO with graceful error handling
    """
    try:
        file_path = BASE_DIR / filename
        
        # Check if file exists
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return None
        
        # Read file content
        raw_data = file_path.read_bytes()
        
        if not raw_data:
            logger.warning(f"File is empty: {file_path}")
            return None
        
        # Create BytesIO object
        buffer = BytesIO(raw_data)
        buffer.seek(0)
        
        logger.info(f"Successfully loaded output from: {file_path}")
        return buffer
        
    except Exception as e:
        logger.error(f"Error loading output from disk: {str(e)}")
        return None

def save_dataframe_to_csv(df: pd.DataFrame, filename: str = "processed_data.csv", 
                         separator: str = ";") -> Tuple[bool, str]:
    """
    Save DataFrame to CSV file with customizable formatting
    """
    try:
        if df is None or df.empty:
            return False, "DataFrame is empty or None"
        
        # Ensure storage directory exists
        if not initialize_storage_directories():
            return False, "Failed to initialize storage directories"
        
        file_path = BASE_DIR / filename
        
        # Save DataFrame with UTF-8 encoding
        df.to_csv(file_path, sep=separator, index=False, encoding="utf-8")
        
        message = f"Successfully saved {len(df)} records to {file_path}"
        logger.info(message)
        return True, message
        
    except Exception as e:
        error_msg = f"Error saving DataFrame to CSV: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def load_dataframe_from_csv(filename: str = "processed_data.csv", 
                           separator: str = ";") -> Optional[pd.DataFrame]:
    """
    Load DataFrame from CSV file with intelligent type detection
    """
    try:
        file_path = BASE_DIR / filename
        
        if not file_path.exists():
            logger.warning(f"CSV file not found: {file_path}")
            return None
        
        # Load DataFrame with appropriate settings
        df = pd.read_csv(file_path, sep=separator, encoding="utf-8", dtype=str)
        
        if df.empty:
            logger.warning(f"Loaded DataFrame is empty: {file_path}")
            return None
        
        logger.info(f"Successfully loaded {len(df)} records from {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading DataFrame from CSV: {str(e)}")
        return None

def save_json_data(data: Dict[str, Any], filename: str = "data.json") -> Tuple[bool, str]:
    """
    Save dictionary data to JSON file with elegant formatting
    """
    try:
        if not data:
            return False, "Data is empty or None"
        
        # Ensure storage directory exists
        if not initialize_storage_directories():
            return False, "Failed to initialize storage directories"
        
        file_path = BASE_DIR / filename
        
        # Save with pretty formatting and UTF-8 encoding
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=2, ensure_ascii=False, default=str)
        
        message = f"Successfully saved JSON data to {file_path}"
        logger.info(message)
        return True, message
        
    except Exception as e:
        error_msg = f"Error saving JSON data: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def load_json_data(filename: str = "data.json") -> Optional[Dict[str, Any]]:
    """
    Load dictionary data from JSON file with error recovery
    """
    try:
        file_path = BASE_DIR / filename
        
        if not file_path.exists():
            logger.warning(f"JSON file not found: {file_path}")
            return None
        
        # Load JSON data
        with open(file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)
        
        logger.info(f"Successfully loaded JSON data from {file_path}")
        return data
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error loading JSON data: {str(e)}")
        return None

def create_backup(source_filename: str, backup_prefix: str = "backup") -> Tuple[bool, str]:
    """
    Create timestamped backup of a file with intelligent naming
    """
    try:
        source_path = BASE_DIR / source_filename
        
        if not source_path.exists():
            return False, f"Source file not found: {source_path}"
        
        # Ensure backup directory exists
        if not initialize_storage_directories():
            return False, "Failed to initialize storage directories"
        
        # Create timestamped backup filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{backup_prefix}_{timestamp}_{source_filename}"
        backup_path = BACKUP_DIR / backup_filename
        
        # Copy file to backup location
        shutil.copy2(str(source_path), str(backup_path))
        
        message = f"Successfully created backup: {backup_path}"
        logger.info(message)
        return True, message
        
    except Exception as e:
        error_msg = f"Error creating backup: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def get_file_info(filename: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed information about a storage file
    """
    try:
        file_path = BASE_DIR / filename
        
        if not file_path.exists():
            return None
        
        stat_info = file_path.stat()
        
        return {
            "filename": filename,
            "full_path": str(file_path),
            "size_bytes": stat_info.st_size,
            "size_mb": round(stat_info.st_size / (1024 * 1024), 2),
            "created": datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
            "modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
            "accessed": datetime.fromtimestamp(stat_info.st_atime).isoformat(),
            "is_file": file_path.is_file(),
            "is_directory": file_path.is_dir()
        }
        
    except Exception as e:
        logger.error(f"Error getting file info: {str(e)}")
        return None

def cleanup_old_files(days_old: int = 30, pattern: str = "backup_*") -> Tuple[bool, str]:
    """
    Clean up old files based on age criteria with safety checks
    """
    try:
        if days_old < 1:
            return False, "days_old must be at least 1"
        
        current_time = datetime.now()
        cutoff_time = current_time.timestamp() - (days_old * 24 * 60 * 60)
        
        files_to_cleanup = []
        
        # Find files matching pattern and age criteria
        for file_path in BASE_DIR.glob(pattern):
            if file_path.is_file():
                file_mtime = file_path.stat().st_mtime
                if file_mtime < cutoff_time:
                    files_to_cleanup.append(file_path)
        
        # Remove old files
        removed_count = 0
        for file_path in files_to_cleanup:
            try:
                file_path.unlink()
                removed_count += 1
                logger.info(f"Removed old file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to remove file {file_path}: {str(e)}")
        
        message = f"Successfully cleaned up {removed_count} old files"
        logger.info(message)
        return True, message
        
    except Exception as e:
        error_msg = f"Error during cleanup: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def get_storage_statistics() -> Dict[str, Any]:
    """
    Get comprehensive statistics about storage usage
    """
    try:
        if not BASE_DIR.exists():
            return {"error": "Storage directory does not exist"}
        
        total_files = 0
        total_size = 0
        file_types = {}
        
        # Recursively analyze all files
        for file_path in BASE_DIR.rglob("*"):
            if file_path.is_file():
                total_files += 1
                file_size = file_path.stat().st_size
                total_size += file_size
                
                # Track file types
                suffix = file_path.suffix.lower()
                if suffix in file_types:
                    file_types[suffix]["count"] += 1
                    file_types[suffix]["size"] += file_size
                else:
                    file_types[suffix] = {"count": 1, "size": file_size}
        
        return {
            "base_directory": str(BASE_DIR),
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "file_types": file_types,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {"error": f"Failed to get storage statistics: {str(e)}"}

# Initialize storage on module import
initialize_storage_directories()

# Export key functions for elegant importing
__all__ = [
    'save_output_to_disk',
    'load_output_from_disk',
    'save_dataframe_to_csv',
    'load_dataframe_from_csv',
    'save_json_data',
    'load_json_data',
    'create_backup',
    'get_file_info',
    'cleanup_old_files',
    'get_storage_statistics',
    'initialize_storage_directories'
]