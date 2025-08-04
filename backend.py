# backend.py - Enhanced Multi-Client Database Operations (Fixed)
"""
The heart of data persistence - orchestrating client isolation with MySQL precision
Handles the symphony of database connections, table operations, and data integrity
across the multi-client architecture with graceful error handling and performance optimization.
"""

import pymysql
import pymysql.cursors
import pandas as pd
import logging
from typing import Tuple, Dict, Any, List, Optional
from datetime import datetime
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration from environment
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'Maracuya123'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True
}

class EnhancedMultiClientDatabase:
    """
    The conductor of multi-client database operations,
    ensuring each client's data remains in perfect isolation
    """
    
    def __init__(self, client_id: str = None):
        self.client_id = client_id
        self.connection = None
        self.cursor = None
    
    def connect(self, database_name: str = None) -> bool:
        """Establish connection to specified database with elegant error handling"""
        try:
            config = DB_CONFIG.copy()
            if database_name:
                config['database'] = database_name
            
            self.connection = pymysql.connect(**config)
            self.cursor = self.connection.cursor()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            return False
    
    def disconnect(self):
        """Close database connections with graceful cleanup"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def get_client_database_name(self, db_type: str) -> str:
        """Generate client-specific database names with consistent patterns"""
        if db_type == "main":
            return f"mapping_validation_{self.client_id}"
        elif db_type == "staging":
            return f"vendor_staging_area_{self.client_id}"
        elif db_type == "catalog":
            return f"product_catalog_{self.client_id}"
        elif db_type == "synonyms":
            return f"synonyms_blacklist_{self.client_id}"
        else:
            raise ValueError(f"Unknown database type: {db_type}")

def test_client_database_connection() -> Tuple[bool, str]:
    """Test the fundamental database connection with system database"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Test system database access
        cursor.execute("USE mapping_validation_system")
        cursor.execute("SELECT COUNT(*) as count FROM user_credentials")
        result = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        return True, f"Connection successful. Found {result['count']} users in system."
    except Exception as e:
        return False, f"Connection failed: {str(e)}"

def get_available_clients() -> List[str]:
    """Retrieve all available client IDs from the database ecosystem with fixed parsing"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Get all client databases - using tuple cursor for SHOW DATABASES
        cursor.execute("SHOW DATABASES LIKE 'mapping_validation_%'")
        databases = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        # Extract client IDs, handling different cursor types
        clients = []
        
        for db_row in databases:
            # Handle both dict and tuple cursor results
            if isinstance(db_row, dict):
                # Dict cursor - key might be 'Database' or something else
                db_name = None
                for key, value in db_row.items():
                    if 'database' in key.lower() or 'db' in key.lower():
                        db_name = value
                        break
                if not db_name and db_row:
                    # Get first value if no clear database column
                    db_name = list(db_row.values())[0]
            elif isinstance(db_row, (tuple, list)):
                # Tuple cursor
                db_name = db_row[0]
            else:
                # Single value
                db_name = str(db_row)
            
            # Extract client ID from database name
            if db_name and db_name.startswith('mapping_validation_'):
                if not db_name.endswith('_system'):  # Exclude system database
                    client_id = db_name.replace('mapping_validation_', '')
                    if client_id not in clients:  # Avoid duplicates
                        clients.append(client_id)
        
        logger.info(f"Found {len(clients)} clients: {clients}")
        return sorted(clients)
        
    except Exception as e:
        logger.error(f"Error retrieving clients: {str(e)}")
        return []

def create_enhanced_client_databases(client_id: str) -> Tuple[bool, str]:
    """Create complete database structure for a new client with all required tables"""
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Define all databases to create
        databases = [
            f"mapping_validation_{client_id}",
            f"vendor_staging_area_{client_id}",
            f"product_catalog_{client_id}",
            f"synonyms_blacklist_{client_id}"
        ]
        
        created_databases = []
        
        for db_name in databases:
            try:
                # Create database
                cursor.execute(f"""
                    CREATE DATABASE IF NOT EXISTS {db_name} 
                    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                """)
                
                # Use database and create appropriate table
                cursor.execute(f"USE {db_name}")
                
                if "mapping_validation_" in db_name:
                    _create_processed_mappings_table(cursor)
                elif "product_catalog_" in db_name:
                    _create_product_catalog_table(cursor)
                elif "synonyms_blacklist_" in db_name:
                    _create_synonyms_blacklist_table(cursor)
                elif "vendor_staging_area_" in db_name:
                    _create_vendor_staging_table(cursor)
                
                created_databases.append(db_name)
                logger.info(f"Successfully created database: {db_name}")
                
            except Exception as db_error:
                logger.error(f"Error creating database {db_name}: {str(db_error)}")
                continue
        
        # Update system tracking
        try:
            cursor.execute("USE mapping_validation_system")
            cursor.execute("""
                INSERT INTO last_updates (client_id, update_type, record_count)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE update_type = VALUES(update_type)
            """, (client_id, "initialized", 0))
        except Exception as track_error:
            logger.warning(f"Could not update tracking for client {client_id}: {str(track_error)}")
        
        cursor.close()
        connection.close()
        
        if created_databases:
            return True, f"Successfully created {len(created_databases)} databases for client: {client_id}"
        else:
            return False, f"Failed to create any databases for client: {client_id}"
            
    except Exception as e:
        logger.error(f"Error in create_enhanced_client_databases: {str(e)}")
        return False, f"Failed to create client databases: {str(e)}"

def _create_processed_mappings_table(cursor):
    """Create the main processed mappings table with complete schema"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_mappings (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            client_id VARCHAR(100) NOT NULL,
            batch_id VARCHAR(100) DEFAULT '',
            vendor_product_description TEXT NOT NULL,
            company_location VARCHAR(255) DEFAULT '',
            vendor_name VARCHAR(255) NOT NULL DEFAULT '',
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
            INDEX idx_created_at (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

def _create_product_catalog_table(cursor):
    """Create the product catalog table for client-specific products"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_catalog (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            categoria VARCHAR(255) DEFAULT '',
            variedad VARCHAR(255) DEFAULT '',
            color VARCHAR(255) DEFAULT '',
            grado VARCHAR(255) DEFAULT '',
            catalog_id VARCHAR(100) NOT NULL,
            search_key TEXT NOT NULL,
            client_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_client_id (client_id),
            INDEX idx_catalog_id (catalog_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

def _create_synonyms_blacklist_table(cursor):
    """Create the synonyms and blacklist management table"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS synonyms_blacklist (
            id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            type VARCHAR(20) NOT NULL,
            original_word VARCHAR(255) DEFAULT NULL,
            synonym_word VARCHAR(255) DEFAULT NULL,
            blacklist_word VARCHAR(255) DEFAULT NULL,
            client_id VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            created_by VARCHAR(100) DEFAULT 'system',
            status VARCHAR(20) DEFAULT 'active',
            INDEX idx_client_id (client_id),
            INDEX idx_type (type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """)

def _create_vendor_staging_table(cursor):
    """Create the vendor staging data table"""
    cursor.execute("""
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
    """)

def load_client_processed_data(client_id: str) -> Optional[pd.DataFrame]:
    """Load all processed data for a specific client with elegant error handling"""
    try:
        db_name = f"mapping_validation_{client_id}"
        connection = pymysql.connect(**DB_CONFIG, database=db_name)
        
        query = """
        SELECT * FROM processed_mappings 
        WHERE client_id = %s 
        ORDER BY created_at DESC
        """
        
        df = pd.read_sql(query, connection, params=(client_id,))
        connection.close()
        
        if len(df) > 0:
            logger.info(f"Loaded {len(df)} records for client {client_id}")
            return df
        else:
            logger.info(f"No records found for client {client_id}")
            return None
            
    except Exception as e:
        logger.error(f"Error loading data for client {client_id}: {str(e)}")
        return None

def save_client_processed_data(client_id: str, df: pd.DataFrame, batch_id: str) -> Tuple[bool, str]:
    """Save processed data to client-specific database with batch tracking"""
    try:
        if df is None or len(df) == 0:
            return False, "No data to save"
        
        db_name = f"mapping_validation_{client_id}"
        connection = pymysql.connect(**DB_CONFIG, database=db_name)
        
        # Add client_id and batch_id to dataframe
        df_to_save = df.copy()
        df_to_save['client_id'] = client_id
        df_to_save['batch_id'] = batch_id
        
        # Map DataFrame columns to database columns
        column_mapping = {
            'Vendor Product Description': 'vendor_product_description',
            'Company Location': 'company_location',
            'Vendor Name': 'vendor_name',
            'Cleaned input': 'cleaned_input',
            'Applied Synonyms': 'applied_synonyms',
            'Removed Blacklist Words': 'removed_blacklist_words',
            'Best match': 'best_match',
            'Similarity %': 'similarity_percentage',
            'Matched Words': 'matched_words',
            'Missing Words': 'missing_words',
            'Catalog ID': 'catalog_id',
            'Categoria': 'categoria',
            'Variedad': 'variedad',
            'Color': 'color',
            'Grado': 'grado',
            'Accept Map': 'accept_map',
            'Deny Map': 'deny_map',
            'Action': 'action',
            'Word': 'word'
        }
        
        # Rename columns to match database schema
        df_to_save = df_to_save.rename(columns=column_mapping)
        
        # Use pandas to_sql for efficient bulk insert
        df_to_save.to_sql('processed_mappings', connection, if_exists='append', 
                         index=False, method='multi', chunksize=1000)
        
        connection.close()
        
        # Update last_updates tracking
        _update_client_timestamp(client_id, "data_saved", len(df))
        
        return True, f"Successfully saved {len(df)} records for client {client_id}"
        
    except Exception as e:
        logger.error(f"Error saving data for client {client_id}: {str(e)}")
        return False, f"Failed to save data: {str(e)}"

def get_client_synonyms_blacklist(client_id: str) -> Dict[str, Any]:
    """Retrieve client-specific synonyms and blacklist with structured format"""
    try:
        db_name = f"synonyms_blacklist_{client_id}"
        connection = pymysql.connect(**DB_CONFIG, database=db_name)
        cursor = connection.cursor()
        
        # Get synonyms
        cursor.execute("""
            SELECT original_word, synonym_word 
            FROM synonyms_blacklist 
            WHERE type = 'synonym' AND status = 'active'
        """)
        synonym_rows = cursor.fetchall()
        synonyms = {row['original_word']: row['synonym_word'] for row in synonym_rows}
        
        # Get blacklist
        cursor.execute("""
            SELECT blacklist_word 
            FROM synonyms_blacklist 
            WHERE type = 'blacklist' AND status = 'active'
        """)
        blacklist_rows = cursor.fetchall()
        blacklist = [row['blacklist_word'] for row in blacklist_rows]
        
        cursor.close()
        connection.close()
        
        return {
            'synonyms': synonyms,
            'blacklist': {'input': blacklist}
        }
        
    except Exception as e:
        logger.error(f"Error loading synonyms/blacklist for client {client_id}: {str(e)}")
        return {'synonyms': {}, 'blacklist': {'input': []}}

def update_client_synonyms_blacklist(client_id: str, synonyms_list: List[Dict], 
                                   blacklist_list: List[str]) -> Tuple[bool, str]:
    """Update client-specific synonyms and blacklist with transaction safety"""
    try:
        db_name = f"synonyms_blacklist_{client_id}"
        connection = pymysql.connect(**DB_CONFIG, database=db_name)
        cursor = connection.cursor()
        
        # Start transaction
        connection.begin()
        
        # Clear existing active records
        cursor.execute("DELETE FROM synonyms_blacklist WHERE client_id = %s", (client_id,))
        
        # Insert new synonyms
        for synonym_dict in synonyms_list:
            for original, replacement in synonym_dict.items():
                cursor.execute("""
                    INSERT INTO synonyms_blacklist 
                    (type, original_word, synonym_word, client_id, created_by)
                    VALUES ('synonym', %s, %s, %s, 'system')
                """, (original, replacement, client_id))
        
        # Insert new blacklist words
        for word in blacklist_list:
            cursor.execute("""
                INSERT INTO synonyms_blacklist 
                (type, blacklist_word, client_id, created_by)
                VALUES ('blacklist', %s, %s, 'system')
            """, (word, client_id))
        
        # Commit transaction
        connection.commit()
        cursor.close()
        connection.close()
        
        return True, f"Updated {len(synonyms_list)} synonyms and {len(blacklist_list)} blacklist words"
        
    except Exception as e:
        logger.error(f"Error updating synonyms/blacklist for client {client_id}: {str(e)}")
        return False, f"Failed to update: {str(e)}"

def get_client_statistics(client_id: str) -> Dict[str, Any]:
    """Generate comprehensive statistics for a client with performance metrics"""
    try:
        db_name = f"mapping_validation_{client_id}"
        connection = pymysql.connect(**DB_CONFIG, database=db_name)
        cursor = connection.cursor()
        
        # Basic statistics
        cursor.execute("SELECT COUNT(*) as total FROM processed_mappings")
        total_records = cursor.fetchone()['total']
        
        cursor.execute("""
            SELECT COUNT(*) as accepted 
            FROM processed_mappings 
            WHERE accept_map = 'True'
        """)
        accepted_records = cursor.fetchone()['accepted']
        
        cursor.execute("""
            SELECT AVG(CAST(similarity_percentage AS DECIMAL(5,2))) as avg_sim
            FROM processed_mappings 
            WHERE similarity_percentage != '' AND similarity_percentage IS NOT NULL
        """)
        avg_similarity = cursor.fetchone()['avg_sim'] or 0
        
        cursor.execute("""
            SELECT COUNT(DISTINCT vendor_name) as vendors
            FROM processed_mappings
        """)
        unique_vendors = cursor.fetchone()['vendors']
        
        cursor.close()
        connection.close()
        
        return {
            'main_stats': {
                'total_records': total_records,
                'accepted_records': accepted_records,
                'unique_vendors': unique_vendors,
                'avg_similarity': float(avg_similarity),
                'acceptance_rate': (accepted_records / total_records * 100) if total_records > 0 else 0
            },
            'client_id': client_id,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting statistics for client {client_id}: {str(e)}")
        return {'error': f"Failed to get statistics: {str(e)}"}

def verify_client_database_structure(client_id: str) -> Tuple[bool, Dict[str, str]]:
    """Verify that all required databases and tables exist for a client"""
    results = {}
    all_good = True
    
    required_databases = {
        f"mapping_validation_{client_id}": "processed_mappings",
        f"product_catalog_{client_id}": "product_catalog",
        f"synonyms_blacklist_{client_id}": "synonyms_blacklist"
    }
    
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        for db_name, table_name in required_databases.items():
            try:
                cursor.execute(f"USE {db_name}")
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if cursor.fetchone():
                    results[db_name] = f"✅ Database and table exist"
                else:
                    results[db_name] = f"❌ Table {table_name} missing"
                    all_good = False
            except Exception as e:
                results[db_name] = f"❌ Database missing or inaccessible: {str(e)}"
                all_good = False
        
        cursor.close()
        connection.close()
        
        return all_good, results
        
    except Exception as e:
        return False, {"error": f"Verification failed: {str(e)}"}

def _update_client_timestamp(client_id: str, update_type: str, record_count: int):
    """Update the last_updates table for real-time sync tracking"""
    try:
        connection = pymysql.connect(**DB_CONFIG, database="mapping_validation_system")
        cursor = connection.cursor()
        
        cursor.execute("""
            INSERT INTO last_updates (client_id, update_type, record_count)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                last_update = CURRENT_TIMESTAMP,
                update_type = VALUES(update_type),
                record_count = VALUES(record_count)
        """, (client_id, update_type, record_count))
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        logger.error(f"Error updating timestamp for client {client_id}: {str(e)}")

def _create_product_catalog_triggers(cursor):
    """Create triggers to automatically populate search_key on insert/update"""
    try:
        cursor.execute("DROP TRIGGER IF EXISTS trg_product_catalog_insert")
        cursor.execute("""
            CREATE TRIGGER trg_product_catalog_insert
            BEFORE INSERT ON product_catalog
            FOR EACH ROW
            SET NEW.search_key = CONCAT_WS(' ', NEW.categoria, NEW.variedad, NEW.color, NEW.grado)
        """)
        
        cursor.execute("DROP TRIGGER IF EXISTS trg_product_catalog_update")
        cursor.execute("""
            CREATE TRIGGER trg_product_catalog_update
            BEFORE UPDATE ON product_catalog
            FOR EACH ROW
            SET NEW.search_key = CONCAT_WS(' ', NEW.categoria, NEW.variedad, NEW.color, NEW.grado)
        """)
        
        logger.info("✅ Triggers for product_catalog created successfully")

    except Exception as e:
        logger.warning(f"⚠️ Failed to create triggers for product_catalog: {str(e)}")

# Export the main class and key functions for easy importing
__all__ = [
    'EnhancedMultiClientDatabase',
    'test_client_database_connection',
    'get_available_clients',
    'create_enhanced_client_databases',
    'load_client_processed_data',
    'save_client_processed_data',
    'get_client_synonyms_blacklist',
    'update_client_synonyms_blacklist',
    'get_client_statistics',
    'verify_client_database_structure',
    '_create_product_catalog_triggers'
]