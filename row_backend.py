# row_backend.py - Enhanced Row-Level Processing Operations
"""
The artisan of individual row transformations - handling precise row-level operations
with graceful fuzzy matching, dynamic synonym application, and staging product creation.
Each row becomes a canvas for intelligent data transformation.
"""

import pandas as pd
from fuzzywuzzy import fuzz, process
from typing import Dict, Any, Tuple, List, Optional
import logging
from datetime import datetime
import pymysql

# Import our backend systems
from backend import EnhancedMultiClientDatabase, get_client_synonyms_blacklist
from ulits import clean_text, apply_synonyms, remove_blacklist, extract_words

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedRowProcessor:
    """
    The master craftsman of row-level transformations,
    wielding fuzzy matching algorithms with precision and grace
    """
    
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.db = EnhancedMultiClientDatabase(client_id)
        
        # Performance cache for catalog data
        self._catalog_cache = None
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes cache TTL
    
    def reprocess_single_row(self, row_data: Dict[str, Any], 
                           update_synonyms_blacklist: bool = True) -> Tuple[bool, Dict[str, Any]]:
        """
        Reprocess a single row with updated synonyms and blacklist,
        applying the full transformation pipeline with elegant precision
        """
        try:
            logger.info(f"Reprocessing row ID: {row_data.get('id', 'unknown')}")
            
            # Update synonyms and blacklist if requested
            if update_synonyms_blacklist:
                self._update_synonyms_blacklist_from_row(row_data)
            
            # Get current client-specific rules
            rules = get_client_synonyms_blacklist(self.client_id)
            
            # Extract and clean original input
            original_input = str(row_data.get('vendor_product_description', ''))
            if not original_input.strip():
                return False, row_data
            
            # Apply transformation pipeline
            cleaned_input = clean_text(original_input)
            
            # Apply synonyms with tracking
            cleaned_with_synonyms, applied_synonyms = apply_synonyms(
                cleaned_input, rules.get('synonyms', {})
            )
            
            # Apply blacklist with tracking
            final_cleaned, removed_blacklist = remove_blacklist(
                cleaned_with_synonyms, rules.get('blacklist', {}).get('input', [])
            )
            
            # Perform enhanced fuzzy matching
            catalog_data = self._get_catalog_data()
            match_result = self._perform_fuzzy_matching(final_cleaned, catalog_data)
            
            # Update row data with transformation results
            updated_row = row_data.copy()
            updated_row.update({
                'cleaned_input': final_cleaned,
                'applied_synonyms': ", ".join([f"{o}→{n}" for o, n in applied_synonyms]),
                'removed_blacklist_words': " ".join(removed_blacklist),
                'best_match': match_result['best_match'],
                'similarity_percentage': match_result['similarity'],
                'matched_words': match_result['matched_words'],
                'missing_words': match_result['missing_words'],
                'catalog_id': match_result['catalog_id'],
                'categoria': match_result['categoria'],
                'variedad': match_result['variedad'],
                'color': match_result['color'],
                'grado': match_result['grado'],
                'updated_at': datetime.now()
            })
            
            logger.info(f"Row reprocessed successfully with {match_result['similarity']}% similarity")
            return True, updated_row
            
        except Exception as e:
            logger.error(f"Error reprocessing row: {str(e)}")
            return False, row_data
    
    def save_as_staging_product(self, row_data: Dict[str, Any], 
                              categoria: str, variedad: str, color: str, grado: str,
                              created_by: str = "system") -> Tuple[bool, str]:
        """
        Save a row as a new staging product with catalog_id 111111,
        marking it for future product creation approval
        """
        try:
            # Connect to product catalog database
            db_name = self.db.get_client_database_name("catalog")
            connection = pymysql.connect(
                **self.db.connection.connection_parameters, 
                database=db_name
            ) if hasattr(self.db, 'connection') and self.db.connection else pymysql.connect(
                host='localhost', user='root', password='Maracuya123', 
                charset='utf8mb4', database=db_name
            )
            
            cursor = connection.cursor()
            
            # Create search key
            search_key = clean_text(f"{categoria} {variedad} {color} {grado}")
            
            # Insert staging product
            insert_query = """
            INSERT INTO product_catalog 
            (categoria, variedad, color, grado, catalog_id, search_key, client_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                categoria, variedad, color, grado, 
                "111111", search_key, self.client_id
            ))
            
            cursor.close()
            connection.close()
            
            # Clear catalog cache to include new staging product
            self._catalog_cache = None
            
            logger.info(f"Staging product created: {categoria}, {variedad}, {color}, {grado}")
            return True, f"Staging product created successfully with ID 111111"
            
        except Exception as e:
            error_msg = f"Error creating staging product: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def update_row_in_database(self, row_id: int, updated_data: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Update a specific row in the database with new field values,
        maintaining data integrity and audit trails
        """
        try:
            # Connect to main database
            db_name = self.db.get_client_database_name("main")
            connection = pymysql.connect(
                host='localhost', user='root', password='Maracuya123',
                charset='utf8mb4', database=db_name
            )
            cursor = connection.cursor()
            
            # Build dynamic update query for allowed fields
            allowed_fields = {
                'cleaned_input', 'applied_synonyms', 'removed_blacklist_words',
                'best_match', 'similarity_percentage', 'matched_words', 'missing_words',
                'catalog_id', 'categoria', 'variedad', 'color', 'grado',
                'accept_map', 'deny_map', 'action', 'word'
            }
            
            update_fields = []
            update_values = []
            
            for field, value in updated_data.items():
                db_field = field.lower().replace(' ', '_')
                if db_field in allowed_fields:
                    update_fields.append(f"{db_field} = %s")
                    update_values.append(str(value) if value is not None else '')
            
            if not update_fields:
                return False, "No valid fields to update"
            
            # Add timestamp update
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            
            update_query = f"""
                UPDATE processed_mappings 
                SET {', '.join(update_fields)}
                WHERE id = %s AND client_id = %s
            """
            
            update_values.extend([row_id, self.client_id])
            
            cursor.execute(update_query, tuple(update_values))
            affected_rows = cursor.rowcount
            
            cursor.close()
            connection.close()
            
            if affected_rows > 0:
                return True, f"Successfully updated row ID {row_id}"
            else:
                return False, f"No row found with ID {row_id}"
                
        except Exception as e:
            error_msg = f"Error updating row in database: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def _update_synonyms_blacklist_from_row(self, row_data: Dict[str, Any]) -> bool:
        """Extract and apply synonym/blacklist updates from row action data"""
        try:
            action = str(row_data.get('action', '')).strip().lower()
            word = str(row_data.get('word', '')).strip()
            
            if not action or not word:
                return True  # No action needed
            
            # Get current rules
            current_rules = get_client_synonyms_blacklist(self.client_id)
            
            # Process based on action type
            if action == 'synonym' and ':' in word:
                # Parse synonym format: "original":"replacement"
                parts = word.split(':', 1)
                original = parts[0].strip().strip('"')
                replacement = parts[1].strip().strip('"')
                
                if original and replacement:
                    # Update synonyms
                    synonyms = current_rules['synonyms'].copy()
                    synonyms[original] = replacement
                    
                    # Convert to list format for update function
                    synonyms_list = [{k: v} for k, v in synonyms.items()]
                    
                    # Update in database
                    from backend import update_client_synonyms_blacklist
                    success, message = update_client_synonyms_blacklist(
                        self.client_id, synonyms_list, current_rules['blacklist']['input']
                    )
                    
                    if success:
                        logger.info(f"Added synonym: {original} → {replacement}")
                    
                    return success
            
            elif action == 'blacklist':
                # Add word to blacklist
                blacklist = current_rules['blacklist']['input'].copy()
                if word not in blacklist:
                    blacklist.append(word)
                    
                    # Convert synonyms to list format
                    synonyms_list = [{k: v} for k, v in current_rules['synonyms'].items()]
                    
                    # Update in database
                    from backend import update_client_synonyms_blacklist
                    success, message = update_client_synonyms_blacklist(
                        self.client_id, synonyms_list, blacklist
                    )
                    
                    if success:
                        logger.info(f"Added to blacklist: {word}")
                    
                    return success
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating synonyms/blacklist from row: {str(e)}")
            return False
    
    def _get_catalog_data(self, force_refresh: bool = False) -> List[Dict[str, Any]]:
        """Get catalog data with intelligent caching for performance optimization"""
        try:
            # Check cache validity
            current_time = datetime.now()
            if (not force_refresh and 
                self._catalog_cache is not None and 
                self._cache_timestamp is not None and
                (current_time - self._cache_timestamp).seconds < self._cache_ttl):
                
                return self._catalog_cache
            
            # Refresh catalog data from database
            db_name = self.db.get_client_database_name("catalog")
            connection = pymysql.connect(
                host='localhost', user='root', password='Maracuya123',
                charset='utf8mb4', database=db_name
            )
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            
            cursor.execute("""
                SELECT categoria, variedad, color, grado, catalog_id, search_key
                FROM product_catalog 
                WHERE client_id = %s
                ORDER BY created_at DESC
            """, (self.client_id,))
            
            results = cursor.fetchall()
            cursor.close()
            connection.close()
            
            # Format for fuzzy matching
            catalog_data = []
            for row in results:
                search_key = row.get('search_key')
                if not search_key:
                    search_key = f"{row['categoria']} {row['variedad']} {row['color']} {row['grado']}".strip()
                    search_key = clean_text(search_key)
                
                catalog_data.append({
                    'search_key': search_key,
                    'categoria': row['categoria'] or '',
                    'variedad': row['variedad'] or '',
                    'color': row['color'] or '',
                    'grado': row['grado'] or '',
                    'catalog_id': row['catalog_id'] or ''
                })
            
            # Update cache
            self._catalog_cache = catalog_data
            self._cache_timestamp = current_time
            
            logger.info(f"Refreshed catalog cache with {len(catalog_data)} entries")
            return catalog_data
            
        except Exception as e:
            logger.error(f"Error getting catalog data: {str(e)}")
            return self._catalog_cache or []
    
    def _perform_fuzzy_matching(self, cleaned_input: str, catalog_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Perform enhanced fuzzy matching with multiple algorithms and fallback strategies,
        orchestrating the dance between input text and catalog possibilities
        """
        try:
            if not cleaned_input.strip() or not catalog_data:
                return self._empty_match_result()
            
            # Extract search keys for fuzzy matching
            search_keys = [item['search_key'] for item in catalog_data if item['search_key']]
            
            if not search_keys:
                return self._empty_match_result()
            
            # Primary fuzzy matching algorithm
            best_match, similarity = process.extractOne(
                cleaned_input, search_keys, scorer=fuzz.token_sort_ratio
            )
            
            # Apply fallback algorithms for improved accuracy
            if similarity < 70:
                alt_match, alt_similarity = process.extractOne(
                    cleaned_input, search_keys, scorer=fuzz.partial_ratio
                )
                if alt_similarity > similarity:
                    best_match, similarity = alt_match, alt_similarity
            
            if similarity < 80:
                token_match, token_similarity = process.extractOne(
                    cleaned_input, search_keys, scorer=fuzz.token_set_ratio
                )
                if token_similarity > similarity:
                    best_match, similarity = token_match, token_similarity
            
            # Find corresponding catalog item
            matched_item = None
            for item in catalog_data:
                if item['search_key'] == best_match:
                    matched_item = item
                    break
            
            if not matched_item:
                return self._empty_match_result()
            
            # Calculate word analysis for insight
            input_words = set(extract_words(cleaned_input))
            match_words = set(extract_words(best_match))
            matched_words = input_words.intersection(match_words)
            missing_words = input_words.difference(match_words)
            
            return {
                'best_match': best_match,
                'similarity': similarity,
                'matched_words': ' '.join(sorted(matched_words)),
                'missing_words': ' '.join(sorted(missing_words)),
                'catalog_id': matched_item['catalog_id'],
                'categoria': matched_item['categoria'],
                'variedad': matched_item['variedad'],
                'color': matched_item['color'],
                'grado': matched_item['grado']
            }
            
        except Exception as e:
            logger.error(f"Error in fuzzy matching: {str(e)}")
            return self._empty_match_result()
    
    def _empty_match_result(self) -> Dict[str, Any]:
        """Return elegantly structured empty match result"""
        return {
            'best_match': '',
            'similarity': 0,
            'matched_words': '',
            'missing_words': '',
            'catalog_id': '',
            'categoria': '',
            'variedad': '',
            'color': '',
            'grado': ''
        }
    
    def clear_cache(self):
        """Clear internal caches for fresh data retrieval"""
        self._catalog_cache = None
        self._cache_timestamp = None
        logger.info("Catalog cache cleared")

# Convenience functions for elegant external access
def enhanced_reprocess_row(client_id: str, row_data: Dict[str, Any], 
                          update_synonyms: bool = True) -> Tuple[bool, Dict[str, Any]]:
    """Convenience function for external row reprocessing"""
    processor = EnhancedRowProcessor(client_id)
    return processor.reprocess_single_row(row_data, update_synonyms)

def enhanced_save_staging_product(client_id: str, row_data: Dict[str, Any], 
                                categoria: str, variedad: str, color: str, grado: str,
                                created_by: str = None) -> Tuple[bool, str]:
    """Convenience function for staging product creation"""
    processor = EnhancedRowProcessor(client_id)
    return processor.save_as_staging_product(
        row_data, categoria, variedad, color, grado, created_by or "system"
    )

def enhanced_update_row_in_database(client_id: str, row_id: int, 
                                  updated_data: Dict[str, Any]) -> Tuple[bool, str]:
    """Convenience function for database row updates"""
    processor = EnhancedRowProcessor(client_id)
    return processor.update_row_in_database(row_id, updated_data)

def get_row_processing_statistics(client_id: str) -> Dict[str, Any]:
    """Get processing statistics for row operations"""
    try:
        processor = EnhancedRowProcessor(client_id)
        catalog_entries = len(processor._get_catalog_data()) if processor._catalog_cache else 0
        
        return {
            'client_id': client_id,
            'catalog_entries': catalog_entries,
            'cache_status': 'active' if processor._catalog_cache else 'empty',
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        return {'error': f"Failed to get statistics: {str(e)}"}

# Export key functions for elegant importing
__all__ = [
    'EnhancedRowProcessor',
    'enhanced_reprocess_row',
    'enhanced_save_staging_product', 
    'enhanced_update_row_in_database',
    'get_row_processing_statistics'
]