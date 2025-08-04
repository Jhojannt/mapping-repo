#!/usr/bin/env python3
"""
Database Initialization Script for Enhanced Multi-Client Data Mapping System

This script creates all necessary databases, tables, and initial data required
for the system to function. It includes:
- System databases and tables
- User management tables with admin user
- Client database structure templates
- Initial test client setup
"""

import pymysql
import pymysql.cursors
import bcrypt
import logging
from datetime import datetime
import sys
import os
from typing import Tuple, List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Maracuya123',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

class DatabaseInitializer:
    """Handles complete database initialization for the system"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = pymysql.connect(**DB_CONFIG)
            self.cursor = self.connection.cursor()
            logger.info("✓ Successfully connected to MySQL server")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to connect to MySQL: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("✓ Disconnected from MySQL server")
    
    def execute_query(self, query: str, params: tuple = None, commit: bool = True) -> bool:
        """Execute a single query with error handling"""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            if commit:
                self.connection.commit()
            return True
        except Exception as e:
            logger.error(f"✗ Query execution failed: {e}")
            if self.connection:
                self.connection.rollback()
            return False
    
    def create_system_database(self) -> bool:
        """Create the main system database"""
        logger.info("Creating system database...")
        
        queries = [
            "CREATE DATABASE IF NOT EXISTS mapping_validation_system CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
            "USE mapping_validation_system"
        ]
        
        for query in queries:
            if not self.execute_query(query):
                return False
        
        logger.info("✓ System database created successfully")
        return True
    
    def create_user_credentials_table(self) -> bool:
        """Create user credentials table for authentication"""
        logger.info("Creating user_credentials table...")
        
        query = """
        CREATE TABLE IF NOT EXISTS user_credentials (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            client_id VARCHAR(100),
            role ENUM('admin', 'client') NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP NULL,
            is_active BOOLEAN DEFAULT TRUE,
            INDEX idx_username (username),
            INDEX idx_client_id (client_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        if self.execute_query(query):
            logger.info("✓ user_credentials table created successfully")
            return True
        return False
    
    def create_last_updates_table(self) -> bool:
        """Create last_updates table for real-time sync"""
        logger.info("Creating last_updates table...")
        
        query = """
        CREATE TABLE IF NOT EXISTS last_updates (
            client_id VARCHAR(100) PRIMARY KEY,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            update_type VARCHAR(50),
            record_count INT DEFAULT 0,
            INDEX idx_last_update (last_update)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        if self.execute_query(query):
            logger.info("✓ last_updates table created successfully")
            return True
        return False
    
    def create_admin_user(self) -> bool:
        """Create the default admin user"""
        logger.info("Creating admin user...")
        
        # Hash the password
        password = "Maracuya123"
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        # Check if admin already exists
        check_query = "SELECT id FROM user_credentials WHERE username = %s"
        self.cursor.execute(check_query, ("Admin",))
        if self.cursor.fetchone():
            logger.info("✓ Admin user already exists")
            return True
        
        # Insert admin user
        insert_query = """
        INSERT INTO user_credentials (username, password_hash, role, is_active)
        VALUES (%s, %s, %s, %s)
        """
        
        if self.execute_query(insert_query, ("Admin", password_hash, "admin", True)):
            logger.info("✓ Admin user created successfully")
            return True
        return False
    
    def create_client_database_structure(self, client_id: str) -> bool:
        """Create complete database structure for a client"""
        logger.info(f"Creating database structure for client: {client_id}")
        
        databases = [
            f"mapping_validation_{client_id}",
            f"vendor_staging_area_{client_id}",
            f"product_catalog_{client_id}",
            f"synonyms_blacklist_{client_id}"
        ]
        
        for db_name in databases:
            # Create database
            create_db_query = f"""
            CREATE DATABASE IF NOT EXISTS {db_name} 
            CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
            """
            
            if not self.execute_query(create_db_query):
                return False
            
            # Use database
            if not self.execute_query(f"USE {db_name}"):
                return False
            
            # Create appropriate tables
            if "mapping_validation_" in db_name:
                if not self._create_processed_mappings_table():
                    return False
            elif "vendor_staging_area_" in db_name:
                if not self._create_vendor_staging_table():
                    return False
            elif "product_catalog_" in db_name:
                if not self._create_product_catalog_table():
                    return False
                if not self._create_product_catalog_triggers():
                    return False
            elif "synonyms_blacklist_" in db_name:
                if not self._create_synonyms_blacklist_table():
                    return False
        
        # Create client user
        if not self._create_client_user(client_id):
            return False
        
        # Add to last_updates
        self.execute_query("USE mapping_validation_system")
        update_query = """
        INSERT INTO last_updates (client_id, update_type, record_count)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE update_type = VALUES(update_type)
        """
        self.execute_query(update_query, (client_id, "initialized", 0))
        
        logger.info(f"✓ Complete database structure created for client: {client_id}")
        return True
    
    def _create_processed_mappings_table(self) -> bool:
        """Create processed_mappings table"""
        query = """
        CREATE TABLE IF NOT EXISTS processed_mappings (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            client_id VARCHAR(100) NOT NULL,
            batch_id VARCHAR(100) DEFAULT '',
            vendor_product_description TEXT NOT NULL,
            company_location VARCHAR(255) DEFAULT '',
            vendor_name VARCHAR(255) NOT NULL DEFAULT '',
            vendor_id VARCHAR(100) DEFAULT '',
            quantity VARCHAR(100) DEFAULT '',
            stems_bunch VARCHAR(100) DEFAULT '',
            unit_type VARCHAR(100) DEFAULT '',
            staging_id VARCHAR(100) DEFAULT '',
            object_mapping_id VARCHAR(100) DEFAULT '',
            company_id VARCHAR(100) DEFAULT '',
            user_id VARCHAR(100) DEFAULT '',
            product_mapping_id VARCHAR(100) DEFAULT '',
            email VARCHAR(255) DEFAULT '',
            cleaned_input TEXT NOT NULL,
            applied_synonyms TEXT,
            removed_blacklist_words TEXT,
            best_match TEXT,
            similarity_percentage VARCHAR(10) DEFAULT '0',
            matched_words TEXT,
            missing_words TEXT,
            catalog_id VARCHAR(100) DEFAULT '',
            categoria VARCHAR(255) DEFAULT '',
            variedad VARCHAR(255) DEFAULT '',
            color VARCHAR(255) DEFAULT '',
            grado VARCHAR(255) DEFAULT '',
            accept_map VARCHAR(10) DEFAULT '',
            deny_map VARCHAR(10) DEFAULT '',
            action VARCHAR(50) DEFAULT '',
            word VARCHAR(255) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_client_id (client_id),
            INDEX idx_vendor_name (vendor_name),
            INDEX idx_similarity (similarity_percentage),
            INDEX idx_created_at (created_at),
            INDEX idx_batch_id (batch_id),
            INDEX idx_catalog_id (catalog_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        return self.execute_query(query)
    
    def _create_vendor_staging_table(self) -> bool:
        """Create vendor_staging_data table"""
        query = """
        CREATE TABLE IF NOT EXISTS vendor_staging_data (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            product_description TEXT NOT NULL,
            column_2 VARCHAR(255) DEFAULT '',
            column_3 VARCHAR(255) DEFAULT '',
            column_4 VARCHAR(255) DEFAULT '',
            column_5 VARCHAR(255) DEFAULT '',
            column_6 VARCHAR(255) DEFAULT '',
            column_7 VARCHAR(255) DEFAULT '',
            column_8 VARCHAR(255) DEFAULT '',
            column_9 VARCHAR(255) DEFAULT '',
            column_10 VARCHAR(255) DEFAULT '',
            column_11 VARCHAR(255) DEFAULT '',
            column_12 VARCHAR(255) DEFAULT '',
            column_13 VARCHAR(255) DEFAULT '',
            client_id VARCHAR(100) NOT NULL,
            batch_id VARCHAR(100) DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_client_id (client_id),
            INDEX idx_batch_id (batch_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        return self.execute_query(query)
    
    def _create_product_catalog_table(self) -> bool:
        """Create product_catalog table"""
        query = """
        CREATE TABLE IF NOT EXISTS product_catalog (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            categoria VARCHAR(255) DEFAULT '',
            variedad VARCHAR(255) DEFAULT '',
            color VARCHAR(255) DEFAULT '',
            grado VARCHAR(255) DEFAULT '',
            additional_field_1 VARCHAR(255) DEFAULT '',
            catalog_id VARCHAR(100) NOT NULL,
            search_key TEXT NOT NULL,
            client_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_client_catalog (client_id, catalog_id),
            INDEX idx_client_id (client_id),
            INDEX idx_catalog_id (catalog_id),
            FULLTEXT KEY ft_search_key (search_key)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        return self.execute_query(query)
    
    def _create_synonyms_blacklist_table(self) -> bool:
        """Create synonyms_blacklist table"""
        query = """
        CREATE TABLE IF NOT EXISTS synonyms_blacklist (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            type VARCHAR(20) NOT NULL,
            original_word VARCHAR(255) DEFAULT NULL,
            synonym_word VARCHAR(255) DEFAULT NULL,
            blacklist_word VARCHAR(255) DEFAULT NULL,
            client_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_by VARCHAR(100) DEFAULT 'admin',
            status VARCHAR(20) DEFAULT 'active',
            INDEX idx_client_id (client_id),
            INDEX idx_type (type),
            INDEX idx_status (status),
            INDEX idx_original_word (original_word),
            INDEX idx_blacklist_word (blacklist_word)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        return self.execute_query(query)
    
    def _create_client_user(self, client_id: str) -> bool:
        """Create a user for the client"""
        # Use system database
        self.execute_query("USE mapping_validation_system")
        
        # Generate username and password
        username = f"client_{client_id}"
        password = f"{client_id}_2024"  # You should generate a secure password
        password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
        
        # Check if user exists
        check_query = "SELECT id FROM user_credentials WHERE username = %s"
        self.cursor.execute(check_query, (username,))
        if self.cursor.fetchone():
            logger.info(f"✓ Client user {username} already exists")
            return True
        
        # Insert client user
        insert_query = """
        INSERT INTO user_credentials (username, password_hash, client_id, role, is_active)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        if self.execute_query(insert_query, (username, password_hash, client_id, "client", True)):
            logger.info(f"✓ Client user created: {username} (password: {password})")
            return True
        return False
    
    def create_test_data(self, client_id: str) -> bool:
        """Create test data for a client"""
        logger.info(f"Creating test data for client: {client_id}")
        
        # Add test product to catalog
        self.execute_query(f"USE product_catalog_{client_id}")
        
        test_products = [
            ("Roses", "Freedom", "Red", "40cm", "", "CAT000001"),
            ("Roses", "Explorer", "Pink", "50cm", "", "CAT000002"),
            ("Carnations", "Standard", "White", "60cm", "", "CAT000003")
        ]
        
        for product in test_products:
            search_key = " ".join(product[:4]).lower()
            insert_query = """
            INSERT INTO product_catalog 
            (categoria, variedad, color, grado, additional_field_1, catalog_id, search_key, client_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE search_key = VALUES(search_key)
            """
            params = product + (search_key, client_id)
            self.execute_query(insert_query, params)
        
        # Add test synonyms
        self.execute_query(f"USE synonyms_blacklist_{client_id}")
        
        test_synonyms = [
            ("rosas", "roses"),
            ("clavel", "carnation"),
            ("cm", "centimeters")
        ]
        
        for original, synonym in test_synonyms:
            insert_query = """
            INSERT INTO synonyms_blacklist 
            (type, original_word, synonym_word, client_id, created_by)
            VALUES ('synonym', %s, %s, %s, 'system')
            """
            self.execute_query(insert_query, (original, synonym, client_id))
        
        # Add test blacklist words
        test_blacklist = ["bunch", "stems", "box"]
        
        for word in test_blacklist:
            insert_query = """
            INSERT INTO synonyms_blacklist 
            (type, blacklist_word, client_id, created_by)
            VALUES ('blacklist', %s, %s, 'system')
            """
            self.execute_query(insert_query, (word, client_id))
        
        logger.info(f"✓ Test data created for client: {client_id}")
        return True
    
    def initialize_complete_system(self) -> bool:
        """Initialize the complete system with all components"""
        logger.info("="*60)
        logger.info("INITIALIZING ENHANCED MULTI-CLIENT DATA MAPPING SYSTEM")
        logger.info("="*60)
        
        # Connect to database
        if not self.connect():
            return False
        
        try:
            # Create system database
            if not self.create_system_database():
                return False
            
            # Create system tables
            if not self.create_user_credentials_table():
                return False
            
            if not self.create_last_updates_table():
                return False
            
            # Create admin user
            if not self.create_admin_user():
                return False
            
            # Create demo client
            demo_client_id = "demo_company"
            if not self.create_client_database_structure(demo_client_id):
                return False
            
            # Add test data for demo client
            if not self.create_test_data(demo_client_id):
                return False
            
            logger.info("="*60)
            logger.info("✓ SYSTEM INITIALIZATION COMPLETED SUCCESSFULLY!")
            logger.info("="*60)
            logger.info("\nLogin Credentials:")
            logger.info("  Admin: Username='Admin', Password='Maracuya123'")
            logger.info(f"  Demo Client: Username='client_{demo_client_id}', Password='{demo_client_id}_2024'")
            logger.info("\nYou can now run the application with: streamlit run login.py")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ System initialization failed: {e}")
            return False
        finally:
            self.disconnect()

def verify_installation() -> bool:
    """Verify that all required packages are installed"""
    logger.info("Verifying required packages...")
    
    required_packages = [
        'pymysql',
        'bcrypt',
        'pandas',
        'streamlit',
        'fuzzywuzzy'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"✗ Missing required packages: {', '.join(missing_packages)}")
        logger.error("Please run: pip install -r requirements.txt")
        return False
    
    logger.info("✓ All required packages are installed")
    return True

def main():
    """Main execution function"""
    print("\n" + "="*60)
    print("ENHANCED MULTI-CLIENT DATA MAPPING SYSTEM")
    print("Database Initialization Script")
    print("="*60 + "\n")
    
    # Verify installation
    if not verify_installation():
        sys.exit(1)
    
    # Confirm before proceeding
    print("This script will:")
    print("1. Create system databases and tables")
    print("2. Create admin user (Admin/Maracuya123)")
    print("3. Create demo client with test data")
    print("4. Set up complete multi-client infrastructure")
    print("\nDatabase: MySQL @ localhost")
    print("User: root")
    print("Password: Maracuya123")
    
    response = input("\nProceed with initialization? (yes/no): ")
    
    if response.lower() != 'yes':
        print("Initialization cancelled.")
        sys.exit(0)
    
    # Initialize system
    initializer = DatabaseInitializer()
    
    if initializer.initialize_complete_system():
        print("\n✓ Database initialization completed successfully!")
        print("\nNext steps:")
        print("1. Ensure MySQL service is running")
        print("2. Run: streamlit run login.py")
        print("3. Login with the credentials shown above")
    else:
        print("\n✗ Database initialization failed!")
        print("Please check the logs above for error details.")
        sys.exit(1)

if __name__ == "__main__":
    main()