# logic.py - Core Data Processing Logic
"""
The symphony conductor of data transformation - orchestrating the harmonious flow
of raw vendor data through intelligent processing pipelines toward structured catalog matches.
Here lies the essence of fuzzy matching artistry and text transformation alchemy.
"""

import pandas as pd
from fuzzywuzzy import fuzz, process
from typing import Optional, Callable, Dict, Any, List
import logging
from datetime import datetime

# Import our utility functions and backend systems
from ulits import clean_text, apply_synonyms, remove_blacklist, extract_words
from backend import get_client_synonyms_blacklist, save_client_processed_data

# Configure logging with poetic precision
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_files(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict, 
                  progress_callback: Optional[Callable] = None, 
                  client_id: Optional[str] = None) -> pd.DataFrame:
    """
    The maestro function - orchestrating the complete data processing symphony
    from raw vendor data to intelligent catalog matches with progress tracking
    """
    logger.info(f"Starting enhanced processing for {len(df1) if df1 is not None else 0} records")
    
    # Validate input integrity
    if df1 is None or df2 is None or df1.empty or df2.empty:
        raise ValueError("Input DataFrames cannot be empty")
    
    if len(df1.columns) < 3:
        raise ValueError("Main DataFrame must have at least 3 columns")
    
    if len(df2.columns) < 4:
        raise ValueError("Catalog DataFrame must have at least 4 columns")
    
    # Create working copies to preserve original data
    df1_work = df1.copy()
    df2_work = df2.copy()
    
    # Define progress update function
    def update_progress(progress_pct: float, message: str = "Processing..."):
        if progress_callback:
            progress_callback(progress_pct, message)
    
    # Extract key columns with elegant indexing
    col_desc = df1_work.columns[0]  # Vendor Product Description
    col_vendor = df1_work.columns[2] if len(df1_work.columns) > 2 else df1_work.columns[1]  # Vendor Name
    
    update_progress(1, "Initializing data processing pipeline...")
    
    # Create deduplication keys for intelligent duplicate handling
    df1_work["duplicate_key"] = (
        df1_work[col_desc].str.strip().str.lower() + "|" + 
        df1_work[col_vendor].str.strip().str.lower()
    )
    
    # Mark duplicates elegantly
    seen = set()
    cleaned_inputs_status = []
    
    for key in df1_work["duplicate_key"]:
        if key in seen:
            cleaned_inputs_status.append("NN")  # Mark as duplicate
        else:
            seen.add(key)
            cleaned_inputs_status.append("PENDING")
    
    duplicate_count = sum(1 for status in cleaned_inputs_status if status == "NN")
    logger.info(f"Identified {duplicate_count} duplicate entries for elegant handling")
    
    update_progress(3, f"Processing {len(df1_work) - duplicate_count} unique records...")
    
    # Get processing rules (client-specific or provided)
    if client_id:
        try:
            client_rules = get_client_synonyms_blacklist(client_id)
            # Merge client rules with provided dictionary (dictionary takes precedence)
            merged_synonyms = {**client_rules.get('synonyms', {}), **dictionary.get("synonyms", {})}
            merged_blacklist = list(set(
                client_rules.get('blacklist', {}).get('input', []) + 
                dictionary.get("blacklist", {}).get("input", [])
            ))
            logger.info(f"Using client-specific rules: {len(merged_synonyms)} synonyms, {len(merged_blacklist)} blacklist words")
        except Exception as e:
            logger.warning(f"Could not load client rules, using provided dictionary: {str(e)}")
            merged_synonyms = dictionary.get("synonyms", {})
            merged_blacklist = dictionary.get("blacklist", {}).get("input", [])
    else:
        merged_synonyms = dictionary.get("synonyms", {})
        merged_blacklist = dictionary.get("blacklist", {}).get("input", [])
    
    update_progress(5, "Applying text transformation pipeline...")
    
    # Apply the transformation pipeline with tracking
    inputs = []
    synonyms_applied = []
    removed_blacklist = []
    
    for idx, row in df1_work.iterrows():
        if cleaned_inputs_status[idx] == "NN":
            # Handle duplicates gracefully
            inputs.append("NN")
            synonyms_applied.append("")
            removed_blacklist.append("")
        else:
            # Extract and clean text
            raw_text = str(row[col_desc])
            cleaned = clean_text(raw_text)
            
            # Apply synonyms with tracking
            cleaned, applied = apply_synonyms(cleaned, merged_synonyms)
            formatted_applied = ", ".join([f"{o}→{n}" for o, n in applied])
            synonyms_applied.append(formatted_applied)
            
            # Apply blacklist with tracking
            cleaned, removed = remove_blacklist(cleaned, merged_blacklist)
            removed_blacklist.append(" ".join(removed))
            
            inputs.append(cleaned.strip())
    
    # Add transformation results to dataframe
    df1_work["Cleaned input"] = inputs
    df1_work["Applied Synonyms"] = synonyms_applied
    df1_work["Removed Blacklist Words"] = removed_blacklist
    
    update_progress(8, "Preparing catalog for intelligent matching...")
    
    # Prepare catalog for fuzzy matching
    concat_cols = df2_work.columns[:4]  # Use first 4 columns for search key
    df2_work["search_key"] = df2_work[concat_cols].fillna("").agg(" ".join, axis=1)
    df2_work["search_key"] = df2_work["search_key"].apply(lambda x: clean_text(x.strip()))
    
    update_progress(10, "Beginning fuzzy matching orchestration...")
    
    # Perform the main fuzzy matching with progress tracking
    results = perform_enhanced_matching(
        df1_work["Cleaned input"].tolist(),
        df2_work,
        progress_callback,
        10,  # Start from 10%
        85   # Use 75% of progress for matching (10% to 85%)
    )
    
    # Apply results to dataframe
    df1_work["Best match"] = results["best_matches"]
    df1_work["Similarity %"] = results["similarities"]
    df1_work["Matched Words"] = results["matched_words"]
    df1_work["Missing Words"] = results["missing_words"]
    df1_work["Catalog ID"] = results["catalog_ids"]
    df1_work["Categoria"] = results["categorias"]
    df1_work["Variedad"] = results["variedades"]
    df1_work["Color"] = results["colores"]
    df1_work["Grado"] = results["grados"]
    
    # Add user action columns if not present
    user_columns = ["Accept Map", "Deny Map", "Action", "Word"]
    for col in user_columns:
        if col not in df1_work.columns:
            df1_work[col] = ""
    
    # Add processing metadata
    if client_id:
        df1_work["Client ID"] = client_id
    df1_work["Processing Timestamp"] = datetime.now()
    
    # Clean up temporary columns
    df1_work.drop(columns=["duplicate_key"], inplace=True, errors='ignore')
    
    update_progress(90, "Finalizing processing results...")
    
    # Save to database if client_id provided
    if client_id:
        try:
            # Generate unique batch ID
            batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Filter out duplicates for database saving
            df_to_save = df1_work[df1_work["Cleaned input"] != "NN"].copy()
            
            if len(df_to_save) > 0:
                success, message = save_client_processed_data(client_id, df_to_save, batch_id)
                if success:
                    logger.info(f"Successfully saved {len(df_to_save)} records to database")
                    update_progress(98, f"Data saved to database: {message}")
                else:
                    logger.warning(f"Database save failed: {message}")
                    update_progress(98, f"Processing complete, database save failed: {message}")
            else:
                update_progress(98, "Processing complete, no valid records to save")
        except Exception as e:
            logger.error(f"Database save error: {str(e)}")
            update_progress(98, f"Processing complete, database error: {str(e)}")
    
    update_progress(100, "Processing symphony completed successfully!")
    
    logger.info(f"Processing completed: {len(df1_work)} total records, {len(df1_work[df1_work['Cleaned input'] != 'NN'])} unique records")
    return df1_work

def perform_enhanced_matching(cleaned_inputs: List[str], df2: pd.DataFrame, 
                            progress_callback: Optional[Callable] = None,
                            start_progress: float = 10, end_progress: float = 85) -> Dict[str, List]:
    """
    Perform enhanced fuzzy matching with intelligent caching and progress tracking,
    creating the delicate dance between input text and catalog possibilities
    """
    logger.info(f"Starting enhanced fuzzy matching for {len(cleaned_inputs)} inputs")
    
    # Initialize result containers
    best_matches = []
    similarities = []
    matched_words = []
    missing_words = []
    catalog_ids = []
    categorias = []
    variedades = []
    colores = []
    grados = []
    
    # Prepare choices for matching
    choices = df2["search_key"].tolist()
    cache = {}  # Performance cache for repeated inputs
    
    total_inputs = len(cleaned_inputs)
    progress_range = end_progress - start_progress
    
    def update_progress(current_idx: int):
        if progress_callback and total_inputs > 0:
            current_progress = start_progress + ((current_idx / total_inputs) * progress_range)
            progress_callback(current_progress, f"Matching records... ({current_idx + 1}/{total_inputs})")
    
    # Main matching loop with elegant progress tracking
    for i, cleaned in enumerate(cleaned_inputs):
        update_progress(i)
        
        if cleaned == "NN":
            # Handle duplicates with grace
            best_matches.append("NN")
            similarities.append("")
            matched_words.append("")
            missing_words.append("")
            catalog_ids.append("")
            categorias.append("")
            variedades.append("")
            colores.append("")
            grados.append("")
        else:
            # Check cache first for performance
            if cleaned not in cache:
                # Perform fuzzy matching with multiple algorithms
                match_result = intelligent_fuzzy_match(cleaned, choices, df2)
                cache[cleaned] = match_result
            
            # Apply cached result
            result = cache[cleaned]
            best_matches.append(result["match"])
            similarities.append(result["similarity"])
            matched_words.append(result["matched"])
            missing_words.append(result["missing"])
            catalog_ids.append(result["catalog_id"])
            categorias.append(result["categoria"])
            variedades.append(result["variedad"])
            colores.append(result["color"])
            grados.append(result["grado"])
    
    logger.info(f"Enhanced matching completed with {len(cache)} unique matches cached")
    
    return {
        "best_matches": best_matches,
        "similarities": similarities,
        "matched_words": matched_words,
        "missing_words": missing_words,
        "catalog_ids": catalog_ids,
        "categorias": categorias,
        "variedades": variedades,
        "colores": colores,
        "grados": grados
    }

def intelligent_fuzzy_match(cleaned_input: str, choices: List[str], df2: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform intelligent fuzzy matching with multiple algorithms and fallback strategies,
    seeking the most harmonious match between input and catalog possibilities
    """
    try:
        # Primary matching algorithm - token_sort_ratio for robust matching
        match, score = process.extractOne(cleaned_input, choices, scorer=fuzz.token_sort_ratio)
        
        # Apply fallback algorithms for enhanced accuracy
        if score < 70:
            # Try partial ratio for substring matching
            alt_match, alt_score = process.extractOne(cleaned_input, choices, scorer=fuzz.partial_ratio)
            if alt_score > score:
                match, score = alt_match, alt_score
        
        if score < 80:
            # Try token set ratio for order-independent matching
            token_match, token_score = process.extractOne(cleaned_input, choices, scorer=fuzz.token_set_ratio)
            if token_score > score:
                match, score = token_match, token_score
        
        # Analyze word overlap for insights
        input_words = set(extract_words(cleaned_input))
        match_words = set(extract_words(match))
        matched = input_words.intersection(match_words)
        missing = input_words.difference(match_words)
        
        # Find corresponding catalog entry
        idx = df2[df2["search_key"] == match].index
        if not idx.empty:
            row_idx = idx[0]
            catalog_id = df2.loc[row_idx, df2.columns[5]] if len(df2.columns) > 5 else ""
            categoria = df2.loc[row_idx, df2.columns[0]]
            variedad = df2.loc[row_idx, df2.columns[1]]
            color = df2.loc[row_idx, df2.columns[2]]
            grado = df2.loc[row_idx, df2.columns[3]]
        else:
            catalog_id = categoria = variedad = color = grado = ""
        
        return {
            "match": match,
            "similarity": score,
            "matched": " ".join(sorted(matched)),
            "missing": " ".join(sorted(missing)),
            "catalog_id": str(catalog_id),
            "categoria": str(categoria),
            "variedad": str(variedad),
            "color": str(color),
            "grado": str(grado)
        }
        
    except Exception as e:
        logger.error(f"Error in intelligent fuzzy matching: {str(e)}")
        return {
            "match": "", "similarity": 0, "matched": "", "missing": "",
            "catalog_id": "", "categoria": "", "variedad": "", "color": "", "grado": ""
        }

def validate_input_data(df1: pd.DataFrame, df2: pd.DataFrame, dictionary: dict) -> tuple[bool, str]:
    """
    Validate input data integrity before processing with elegant error reporting
    """
    try:
        # Check DataFrame validity
        if df1 is None or df1.empty:
            return False, "Main DataFrame is empty or None"
        
        if df2 is None or df2.empty:
            return False, "Catalog DataFrame is empty or None"
        
        # Check column requirements
        if len(df1.columns) < 3:
            return False, f"Main DataFrame needs at least 3 columns, found {len(df1.columns)}"
        
        if len(df2.columns) < 4:
            return False, f"Catalog DataFrame needs at least 4 columns, found {len(df2.columns)}"
        
        # Validate dictionary structure
        if not isinstance(dictionary, dict):
            return False, "Dictionary must be a valid dict object"
        
        # Check synonyms structure
        synonyms = dictionary.get("synonyms", {})
        if not isinstance(synonyms, dict):
            return False, "Dictionary 'synonyms' must be a dict"
        
        # Check blacklist structure
        blacklist = dictionary.get("blacklist", {})
        if not isinstance(blacklist, dict):
            return False, "Dictionary 'blacklist' must be a dict"
        
        blacklist_input = blacklist.get("input", [])
        if not isinstance(blacklist_input, list):
            return False, "Dictionary 'blacklist.input' must be a list"
        
        return True, "Input validation passed successfully"
        
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def create_processing_summary(df: pd.DataFrame, client_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Create an elegant summary of processing results with statistical insights
    """
    try:
        total_rows = len(df)
        valid_rows = len(df[df["Cleaned input"] != "NN"])
        duplicate_rows = total_rows - valid_rows
        
        # Calculate similarity statistics
        similarities = df[df["Similarity %"] != ""]["Similarity %"]
        if len(similarities) > 0:
            similarities_numeric = pd.to_numeric(similarities, errors='coerce')
            avg_similarity = similarities_numeric.mean()
            min_similarity = similarities_numeric.min()
            max_similarity = similarities_numeric.max()
        else:
            avg_similarity = min_similarity = max_similarity = 0
        
        # Count staging products (catalog_id "111111")
        needs_creation = len(df[df["Catalog ID"].astype(str).str.contains("111111", na=False)])
        
        # User action statistics
        accepted = len(df[df["Accept Map"].astype(str).str.lower() == "true"])
        denied = len(df[df["Deny Map"].astype(str).str.lower() == "true"])
        pending = valid_rows - accepted - denied
        
        return {
            "client_id": client_id,
            "timestamp": datetime.now().isoformat(),
            "totals": {
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "duplicate_rows": duplicate_rows,
                "staging_products": needs_creation
            },
            "similarity_stats": {
                "average": round(float(avg_similarity), 2) if not pd.isna(avg_similarity) else 0,
                "minimum": round(float(min_similarity), 2) if not pd.isna(min_similarity) else 0,
                "maximum": round(float(max_similarity), 2) if not pd.isna(max_similarity) else 0
            },
            "user_actions": {
                "accepted": accepted,
                "denied": denied,
                "pending_review": pending
            }
        }
        
    except Exception as e:
        logger.error(f"Error creating processing summary: {str(e)}")
        return {
            "error": str(e),
            "client_id": client_id,
            "timestamp": datetime.now().isoformat()
        }

# Export key functions for elegant importing
__all__ = [
    'process_files',
    'perform_enhanced_matching',
    'intelligent_fuzzy_match',
    'validate_input_data',
    'create_processing_summary'
]