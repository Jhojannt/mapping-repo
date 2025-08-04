# ulits.py - Core Utility Functions
"""
The foundational artistry of text transformation - where raw strings become refined data
through careful cleaning, intelligent synonym application, and precise blacklist filtering.
These utilities form the bedrock of our data processing symphony.
"""

import re
import unicodedata
from typing import Tuple, List, Dict, Any
import pandas as pd

def clean_text(text: str) -> str:
    """
    Transform raw text into its purest form through systematic cleaning,
    removing the chaos of inconsistent formatting to reveal structured beauty
    """
    if not isinstance(text, str):
        text = str(text)
    
    # Normalize Unicode characters - converting accented characters to base forms
    text = unicodedata.normalize("NFD", text)
    
    # Remove accent marks while preserving base characters
    text = text.encode("ascii", "ignore").decode("utf-8")
    
    # Remove punctuation and special characters, replacing with spaces
    text = re.sub(r"[^\w\s]", " ", text)
    
    # Collapse multiple whitespace into single spaces
    text = re.sub(r"\s+", " ", text)
    
    # Return cleaned text in lowercase with trimmed edges
    return text.strip().lower()

def apply_synonyms(text: str, synonyms: Dict[str, str]) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Apply intelligent synonym replacement with case-insensitive matching,
    tracking each transformation for complete transparency and auditability
    """
    if not isinstance(text, str) or not isinstance(synonyms, dict):
        return str(text), []
    
    # Create case-insensitive synonym mapping
    synonyms_lower = {k.lower(): v for k, v in synonyms.items()}
    
    # Split text into words for individual processing
    words = text.split()
    replaced_words = []
    applied_transformations = []
    
    # Process each word with synonym intelligence
    for word in words:
        word_key = word.lower()
        if word_key in synonyms_lower:
            replacement = synonyms_lower[word_key]
            replaced_words.append(replacement)
            applied_transformations.append((word, replacement))
        else:
            replaced_words.append(word)
    
    # Return transformed text and transformation log
    return " ".join(replaced_words), applied_transformations

def remove_blacklist(text: str, blacklist: List[str]) -> Tuple[str, List[str]]:
    """
    Remove unwanted words and phrases with surgical precision,
    ensuring only complete words are removed while preserving text integrity
    """
    if not isinstance(text, str) or not isinstance(blacklist, list):
        return str(text), []
    
    removed_items = []
    working_text = text
    
    # Sort blacklist by length (longest first) to handle phrases before individual words
    blacklist_sorted = sorted(blacklist, key=lambda x: -len(x.strip()))
    
    # Process each blacklist item with word boundary awareness
    for phrase in blacklist_sorted:
        phrase_clean = phrase.strip()
        if not phrase_clean:
            continue
            
        # Create regex pattern with word boundaries for complete word matching
        pattern = r'\b' + re.escape(phrase_clean) + r'\b'
        
        # Check if pattern exists before removal
        if re.search(pattern, working_text, flags=re.IGNORECASE):
            removed_items.append(phrase_clean)
            working_text = re.sub(pattern, '', working_text, flags=re.IGNORECASE)
    
    # Clean up extra spaces created by removals
    cleaned_text = " ".join(working_text.strip().split())
    
    return cleaned_text, removed_items

def extract_words(text: str) -> List[str]:
    """
    Extract individual words from text for fuzzy matching analysis,
    creating a foundation for intelligent similarity calculations
    """
    if not isinstance(text, str):
        text = str(text)
    
    # Extract alphanumeric words using regex, converting to lowercase
    words = re.findall(r"\b\w+\b", text.lower())
    
    # Return unique words while preserving order
    seen = set()
    unique_words = []
    for word in words:
        if word not in seen and len(word) > 1:  # Filter out single characters
            seen.add(word)
            unique_words.append(word)
    
    return unique_words

def classify_missing_words(words_str: str, classification_dict: Dict[str, List[str]]) -> str:
    """
    Classify missing words into semantic categories for enhanced insights,
    providing intelligent categorization of unmatched terms
    """
    if pd.isna(words_str) or not isinstance(words_str, str) or words_str.strip() == "":
        return ""
    
    categories = []
    words = words_str.strip().split()
    
    # Process each word through classification system
    for word in words:
        word_lower = word.lower()
        matched = False
        
        # Check against each category in classification dictionary
        for category, category_words in classification_dict.items():
            if isinstance(category_words, list):
                category_words_lower = [w.lower() for w in category_words]
                if word_lower in category_words_lower:
                    categories.append(category)
                    matched = True
                    break
        
        # Mark unclassified words
        if not matched:
            categories.append("unclassified")
    
    # Return unique categories as comma-separated string
    return ", ".join(sorted(set(categories)))

def normalize_similarity_score(score: Any, default_value: float = 0.0) -> float:
    """
    Normalize similarity scores to consistent float values with elegant error handling
    """
    try:
        if pd.isna(score) or score == "" or score is None:
            return default_value
        
        # Convert string representations to float
        if isinstance(score, str):
            # Remove percentage symbols and extra whitespace
            score = score.replace('%', '').strip()
            if score == "":
                return default_value
        
        # Convert to float with bounds checking
        float_score = float(score)
        
        # Ensure score is within valid range (0-100)
        return max(0.0, min(100.0, float_score))
        
    except (ValueError, TypeError):
        return default_value

def validate_text_input(text: Any) -> str:
    """
    Validate and normalize text input with graceful handling of various data types
    """
    if text is None or pd.isna(text):
        return ""
    
    if isinstance(text, (int, float)):
        return str(text)
    
    if isinstance(text, str):
        return text.strip()
    
    # Handle other data types by converting to string
    try:
        return str(text).strip()
    except Exception:
        return ""

def create_search_key(categoria: str, variedad: str, color: str, grado: str) -> str:
    """
    Create a standardized search key from product attributes,
    ensuring consistent format for fuzzy matching operations
    """
    # Validate and clean each component
    components = [
        validate_text_input(categoria),
        validate_text_input(variedad), 
        validate_text_input(color),
        validate_text_input(grado)
    ]
    
    # Filter out empty components and join with spaces
    valid_components = [comp for comp in components if comp]
    combined_text = " ".join(valid_components)
    
    # Apply cleaning for consistency
    return clean_text(combined_text)

def safe_dataframe_column_access(df: pd.DataFrame, column_identifier: Any, 
                                default_value: str = "") -> pd.Series:
    """
    Safely access DataFrame columns with intelligent fallback handling
    """
    try:
        if df is None or df.empty:
            return pd.Series(dtype=str)
        
        # Handle numeric column indices
        if isinstance(column_identifier, int):
            if 0 <= column_identifier < len(df.columns):
                return df.iloc[:, column_identifier].fillna(default_value)
            else:
                return pd.Series([default_value] * len(df), dtype=str)
        
        # Handle string column names
        elif isinstance(column_identifier, str):
            if column_identifier in df.columns:
                return df[column_identifier].fillna(default_value)
            else:
                return pd.Series([default_value] * len(df), dtype=str)
        
        # Fallback for other types
        else:
            return pd.Series([default_value] * len(df), dtype=str)
            
    except Exception:
        # Ultimate fallback
        return pd.Series([default_value] * (len(df) if df is not None else 0), dtype=str)

def format_percentage(value: Any, decimal_places: int = 1) -> str:
    """
    Format numerical values as elegant percentage strings with consistent formatting
    """
    try:
        # Normalize the input value
        normalized = normalize_similarity_score(value, 0.0)
        
        # Format with specified decimal places
        return f"{normalized:.{decimal_places}f}%"
        
    except Exception:
        return "0.0%"

def extract_numeric_from_string(text: str) -> float:
    """
    Extract numeric values from mixed text strings with intelligent parsing
    """
    if not isinstance(text, str):
        text = str(text)
    
    # Find all numeric patterns in the string
    numeric_pattern = r'-?\d+\.?\d*'
    matches = re.findall(numeric_pattern, text)
    
    if matches:
        try:
            # Return the first numeric value found
            return float(matches[0])
        except ValueError:
            return 0.0
    
    return 0.0

def truncate_text(text: str, max_length: int = 50, ellipsis: str = "...") -> str:
    """
    Intelligently truncate text while preserving readability and context
    """
    if not isinstance(text, str):
        text = str(text)
    
    if len(text) <= max_length:
        return text
    
    # Find the last space before the limit to avoid cutting words
    truncate_point = max_length - len(ellipsis)
    
    # Look for word boundary near truncation point
    space_index = text.rfind(' ', 0, truncate_point)
    
    if space_index > max_length * 0.7:  # If space is reasonably close to limit
        return text[:space_index] + ellipsis
    else:
        return text[:truncate_point] + ellipsis

def create_audit_trail_entry(action: str, details: Dict[str, Any], 
                           user: str = "system") -> Dict[str, Any]:
    """
    Create standardized audit trail entries for tracking system operations
    """
    return {
        "timestamp": pd.Timestamp.now().isoformat(),
        "action": action,
        "user": user,
        "details": details,
        "version": "1.0"
    }

def batch_process_list(items: List[Any], batch_size: int = 100) -> List[List[Any]]:
    """
    Divide large lists into manageable batches for efficient processing
    """
    if not isinstance(items, list) or batch_size <= 0:
        return [items] if items else []
    
    batches = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        if batch:  # Only add non-empty batches
            batches.append(batch)
    
    return batches

def merge_dictionaries(*dicts: Dict[str, Any], 
                      conflict_strategy: str = "last_wins") -> Dict[str, Any]:
    """
    Intelligently merge multiple dictionaries with configurable conflict resolution
    """
    if not dicts:
        return {}
    
    merged = {}
    
    for dictionary in dicts:
        if not isinstance(dictionary, dict):
            continue
            
        for key, value in dictionary.items():
            if key not in merged:
                merged[key] = value
            else:
                # Handle conflicts based on strategy
                if conflict_strategy == "last_wins":
                    merged[key] = value
                elif conflict_strategy == "first_wins":
                    pass  # Keep existing value
                elif conflict_strategy == "combine_lists":
                    if isinstance(merged[key], list) and isinstance(value, list):
                        merged[key] = merged[key] + value
                    else:
                        merged[key] = value
    
    return merged

# Export all utility functions for elegant importing
__all__ = [
    'clean_text',
    'apply_synonyms', 
    'remove_blacklist',
    'extract_words',
    'classify_missing_words',
    'normalize_similarity_score',
    'validate_text_input',
    'create_search_key',
    'safe_dataframe_column_access',
    'format_percentage',
    'extract_numeric_from_string',
    'truncate_text',
    'create_audit_trail_entry',
    'batch_process_list',
    'merge_dictionaries'
]