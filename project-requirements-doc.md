# Enhanced Multi-Client Data Mapping System - Requirements Documentation

## Project Overview

The Enhanced Multi-Client Data Mapping System is a sophisticated web application designed to process vendor product lists and match them with a company's catalog products using advanced fuzzy matching algorithms. The system provides complete client isolation, real-time data processing, comprehensive administration capabilities, and secure multi-user authentication.

## Core Functionalities

### 1. Data Processing Pipeline

#### Input Requirements
- **Main TSV File**: Vendor product descriptions and metadata
- **Catalog TSV File**: Company's current product catalog
- **Dictionary JSON**: Synonyms and blacklist configurations

#### Processing Steps
1. **Text Cleaning**
   - Remove accents and special characters
   - Normalize whitespace
   - Convert to lowercase
   - Apply phrase-level removal with case-insensitive matching

2. **Synonym Application**
   - Client-specific synonym replacement
   - Global synonym rules
   - Track applied synonyms for audit trail

3. **Blacklist Removal**
   - Remove unwanted words/phrases
   - Client-specific blacklist
   - Track removed words

4. **Fuzzy Matching**
   - Token-based similarity scoring
   - Multiple algorithm fallback (token_sort_ratio, partial_ratio, token_set_ratio)
   - Combined catalog matching (master + staging products)
   - Cache optimization for performance

### 2. Database Architecture

#### Multi-Client Structure
Each client has isolated databases:

1. **mapping_validation_{client_id}**
   - Table: `processed_mappings`
   - Stores all processing results with full audit trail

2. **vendor_staging_area_{client_id}**
   - Table: `vendor_staging_data`
   - Staging area for vendor data

3. **product_catalog_{client_id}**
   - Table: `product_catalog`
   - Client's product catalog

4. **synonyms_blacklist_{client_id}**
   - Table: `synonyms_blacklist`
   - Client-specific text processing rules

#### Database Schema Details

**processed_mappings** (Production Table):
```sql
- id (BIGINT AUTO_INCREMENT PRIMARY KEY)
- client_id (VARCHAR(100))
- batch_id (VARCHAR(100))
- vendor_product_description (TEXT)
- company_location (VARCHAR(255))
- vendor_name (VARCHAR(255))
- vendor_id (VARCHAR(100))
- quantity (VARCHAR(100))
- stems_bunch (VARCHAR(100))
- unit_type (VARCHAR(100))
- staging_id (VARCHAR(100))
- object_mapping_id (VARCHAR(100))
- company_id (VARCHAR(100))
- user_id (VARCHAR(100))
- product_mapping_id (VARCHAR(100))
- email (VARCHAR(255))
- cleaned_input (TEXT)
- applied_synonyms (TEXT)
- removed_blacklist_words (TEXT)
- best_match (TEXT)
- similarity_percentage (VARCHAR(10))
- matched_words (TEXT)
- missing_words (TEXT)
- catalog_id (VARCHAR(100))
- categoria (VARCHAR(255))
- variedad (VARCHAR(255))
- color (VARCHAR(255))
- grado (VARCHAR(255))
- accept_map (VARCHAR(10))
- deny_map (VARCHAR(10))
- action (VARCHAR(50))
- word (VARCHAR(255))
- created_at (TIMESTAMP)
- updated_at (TIMESTAMP)
```

**product_catalog**:
```sql
- id (BIGINT AUTO_INCREMENT PRIMARY KEY)
- categoria (VARCHAR(255))
- variedad (VARCHAR(255))
- color (VARCHAR(255))
- grado (VARCHAR(255))
- additional_field_1 (VARCHAR(255))
- catalog_id (VARCHAR(100))
- search_key (TEXT)
- client_id (VARCHAR(100))
- created_at (TIMESTAMP)
- updated_at (TIMESTAMP)
```

**synonyms_blacklist**:
```sql
- id (BIGINT AUTO_INCREMENT PRIMARY KEY)
- type (VARCHAR(20)) -- 'synonym' or 'blacklist'
- original_word (VARCHAR(255))
- synonym_word (VARCHAR(255))
- blacklist_word (VARCHAR(255))
- client_id (VARCHAR(100))
- created_at (TIMESTAMP)
- updated_at (TIMESTAMP)
- created_by (VARCHAR(100))
- status (VARCHAR(20))
```

### 3. User Interface Features

#### Client Panel (streamlit_app.py)

1. **Data Viewing**
   - Load processed data by client ID
   - Display columns: cleaned_input, similarity_percentage, best_match, Category, Variety, Color, Grade, Accept, Deny
   - Checkbox state persistence in database
   - Real-time updates

2. **Row-Level Operations**
   - Edit button per row opening modal with:
     - Vendor Product Description
     - Vendor Name
     - Unit Type
     - Applied Synonyms
     - Removed Blacklist Words
     - Similarity Percentage
     - Missing Words
   - Update Category, Variety, Color, Grade
   - Re-run fuzzy matching for individual rows
   - Save as new staging product

3. **Filtering Capabilities**
   - Similarity percentage range slider (1-100%)
   - Search across all columns
   - Column-specific exclusion filters
   - Include/exclude by specific column values
   - Persistent filter state

4. **Progress Tracking**
   - Processing progress bar (90% for row processing)
   - Review progress bar (accepted rows / total rows)
   - Animated liquid gradient effects
   - Real-time updates

5. **Bulk Operations**
   - Accept all visible rows
   - Deny all visible rows
   - Clear all selections
   - Save all to database

#### Admin Panel (1admin_interface.py)

1. **Client Management**
   - Create new clients with full database structure
   - View client list and statistics
   - Client-specific metrics:
     - Total records
     - Accepted records
     - Unique vendors
     - Average similarity
     - Last activity

2. **Database Operations**
   - Backup client data to JSON
   - Delete client (with confirmation)
   - Optimize databases
   - View performance metrics
   - Database size monitoring

3. **System Analytics**
   - Processing volume charts
   - Accuracy trends
   - Active client monitoring
   - Storage usage tracking

### 4. Enhanced Features

#### Staging Products System
- Products with catalog_id "111111" marked for creation
- Dedicated staging area management
- Approval workflow
- Export capabilities

#### Synonyms & Blacklist Management
- Client-specific rules
- Real-time addition/removal
- View current configurations
- Clear all functionality

#### Row Reprocessing
- Individual row fuzzy matching refresh
- Uses updated synonyms/blacklist
- Maintains audit trail

### 5. Security & Access Control

#### Authentication System
- **Database-backed user management**
  - Table: `user_credentials`
    - id (INT AUTO_INCREMENT PRIMARY KEY)
    - username (VARCHAR(100) UNIQUE)
    - password_hash (VARCHAR(255))
    - client_id (VARCHAR(100))
    - role (ENUM('admin', 'client'))
    - created_at (TIMESTAMP)
    - last_login (TIMESTAMP)
    - is_active (BOOLEAN)
- **Admin user**: Username "Admin", Password "Maracuya123"
- **Client users**: Each client has dedicated login credentials
- **Role-based access control**:
  - Admin: Access to admin_app.py only
  - Client: Access to client_app.py with their data only
- **Session management** with secure login/logout
- **Password hashing** using bcrypt
- **Client-based data isolation**
- **Session state management**
- **Database connection status monitoring**

### 6. Technical Requirements

#### Error Handling
- Comprehensive try-catch blocks
- Detailed logging with context
- User-friendly error messages
- Graceful degradation

#### Performance Optimization
- Fuzzy matching cache
- Batch processing
- Progress callbacks
- Efficient DataFrame operations

#### Deployment Ready
- Streamlit configuration
- Environment variables (.env)
- MySQL connection pooling
- Render.com compatibility

### 7. Real-Time Features

#### Auto-Update Mechanism
- **Automatic refresh every 30 seconds**
- Check for data updates in background
- Last update tracking per client
- Table: `last_updates`
  - client_id (VARCHAR(100) PRIMARY KEY)
  - last_update (TIMESTAMP)
  - update_type (VARCHAR(50))
  - record_count (INT)
- Automatic data refresh without losing user state
- Session state synchronization
- Non-disruptive updates (preserves filters and selections)

#### Live Database Integration
- Immediate persistence of changes
- Real-time status updates
- Connection monitoring
- Automatic reconnection

### 8. User Experience Enhancements

#### Visual Design
- Gradient progress bars with animations
- Status indicators with color coding
- Modal dialogs for complex operations
- Responsive layout
- Dark/light theme support

#### Workflow Optimization
- Pagination for large datasets
- Keyboard shortcuts
- Bulk operations
- Export capabilities
- Filter persistence

## Missing/Enhanced Features Beyond Original Requirements

1. **Enhanced Fuzzy Matching**
   - Multiple algorithm fallback
   - Combined catalog (master + staging)
   - Performance caching

2. **Comprehensive Admin Dashboard**
   - System-wide statistics
   - Performance monitoring
   - Bulk client operations

3. **Advanced Filtering**
   - Multi-column search
   - Exclusion filters
   - Filter combinations

4. **Audit Trail**
   - Complete processing history
   - User action tracking
   - Timestamp logging

5. **Export Capabilities**
   - CSV export for staging products
   - Backup to JSON
   - Filtered data export

## Database Connection Configuration

```python
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=Maracuya123
DB_NAME=mapping_validation_consolidated
```

## File Structure Compliance

The implementation follows the requested structure:

```
mapping_app/
├── login.py              # Authentication interface
├── client_app.py         # Main client interface (formerly streamlit_app.py)
├── admin_app.py          # Admin panel (formerly 1admin_interface.py)
├── backend.py            # Database operations (formerly Enhanced_MultiClient_Database.py)
├── row_backend.py        # Row-level operations (formerly row_level_processing.py)
├── logic.py              # Core processing logic
├── ulits.py              # Utility functions
├── requirements.txt      # Dependencies
├── .env                  # Configuration
├── storage.py            # File storage operations
└── assets/
    └── styles.css        # All CSS styling separated
```

## Deployment Checklist

- [x] Streamlit compatible
- [x] MySQL database integration
- [x] Environment variable configuration
- [x] Error handling implementation
- [x] Multi-client isolation
- [x] Real-time updates
- [x] Progress tracking
- [x] Authentication system
- [x] Render.com ready

## Additional System Tables

### User Management Tables

**user_credentials**:
```sql
CREATE TABLE user_credentials (
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
);
```

**last_updates**:
```sql
CREATE TABLE last_updates (
    client_id VARCHAR(100) PRIMARY KEY,
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    update_type VARCHAR(50),
    record_count INT DEFAULT 0,
    INDEX idx_last_update (last_update)
);
```

## System Architecture Updates

### Authentication Flow
1. User accesses `login.py`
2. Enters credentials (username/password)
3. System validates against `user_credentials` table
4. Based on role:
   - Admin → Redirects to `admin_app.py`
   - Client → Redirects to `client_app.py` with client_id filter
5. Session state maintains authentication
6. Logout clears session and returns to login

### CSS Organization
All styling moved to `assets/styles.css`:
- Main container styles
- Progress bar animations
- Modal styling
- Button hover effects
- Responsive design rules
- Theme variables (dark/light mode)

### Auto-Update Implementation
- Background thread checks `last_updates` table every 30 seconds
- Compares with local timestamp
- If newer data exists:
  - Preserves current filters and selections
  - Reloads data in background
  - Updates UI without disruption
- Updates status indicator in UI