# client_app.py - Complete Enhanced Multi-Client Data Processing Interface
"""
The comprehensive symphony of client data transformation - orchestrating every nuance
of the original streamlit_app.py with enhanced multi-client isolation and refined aesthetics.
Where sophisticated file processing meets elegant user interaction design.
"""

import streamlit as st
import pandas as pd
import json
from io import BytesIO
import time
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

# Import our orchestrated backend systems
from backend import (
    get_available_clients,
    create_enhanced_client_databases,
    load_client_processed_data,
    save_client_processed_data,
    get_client_synonyms_blacklist,
    update_client_synonyms_blacklist,
    test_client_database_connection,
    verify_client_database_structure,
    get_client_statistics
)
from logic import process_files, validate_input_data
from row_backend import (
    enhanced_reprocess_row,
    enhanced_save_staging_product,
    enhanced_update_row_in_database
)
from storage import save_dataframe_to_csv, save_output_to_disk, load_output_from_disk


# Configure logging with precision
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_session_state():
    """Initialize the symphony of session state variables with robust defaults"""
    session_vars = {
        # Client orchestration
        'current_client_id': None,
        'available_clients': [],
        'show_client_setup': False,
        'new_client_id': '',
        
        # Data choreography
        'processed_data': None,
        'form_data': {},
        'dark_mode': False,
        'db_connection_status': None,
        
        # Enhanced modal symphonies
        'show_edit_product_modal': False,
        'edit_product_row_data': None,
        'edit_product_row_index': None,
        'edit_categoria': '',
        'edit_variedad': '',
        'edit_color': '',
        'edit_grado': '',
        'edit_action': '',
        'edit_word': '',
        
        # Progress tracking rhythms
        'reviewed_count': 0,
        'total_rows': 0,
        'show_progress': True,
        'progress_percentage': 0.0,
        
        # File processing crescendos
        'uploaded_files': {},
        'processing_status': 'ready',
        
        # Filtering melodies
        'current_page': 1,
        'search_text': '',
        'similarity_range': (1, 100),
        'selected_exclusion_column': 'None',
        'exclusion_filter_value': '',
        'rows_per_page': 50,
        'filter_column': 'None',
        'filter_column_index': 0,
        'filter_value': '',
        
        # Message harmonics
        'success_message': '',
        'error_message': '',
        'info_message': '',
        'warning_message': ''
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value

def check_client_authentication():
    """Verify client authentication with graceful redirection"""
    if not st.session_state.get('authenticated', False):
        st.error("Authentication required. Please login first.")
        if st.button("Return to Login"):
            st.switch_page("login.py")
        st.stop()

def check_database_connection():
    """Test database connection using enhanced multi-client system"""
    try:
        success, message = test_client_database_connection()
        if success:
            st.session_state.db_connection_status = "connected"
            return True
        else:
            st.session_state.db_connection_status = f"failed: {message}"
            return False
    except Exception as e:
        st.session_state.db_connection_status = f"error: {str(e)}"
        return False

def database_status_widget():
    """Display database connection status widget with enhanced styling"""
    if st.session_state.db_connection_status is None:
        status_class = "background: linear-gradient(135deg, #ffc10722, #fd7e1422); border: 1px solid #ffc107; color: #ffc107;"
        status_text = "Database Status: Not Tested"
        icon = "🔍"
    elif st.session_state.db_connection_status == "connected":
        status_class = "background: linear-gradient(135deg, #28a74522, #20c99722); border: 1px solid #28a745; color: #28a745;"
        status_text = "Database Status: Connected"
        icon = "✅"
    elif st.session_state.db_connection_status.startswith("failed"):
        status_class = "background: linear-gradient(135deg, #dc354522, #c8211022); border: 1px solid #dc3545; color: #dc3545;"
        status_text = "Database Status: Connection Failed"
        icon = "❌"
    else:
        status_class = "background: linear-gradient(135deg, #dc354522, #c8211022); border: 1px solid #dc3545; color: #dc3545;"
        status_text = f"Database Status: {st.session_state.db_connection_status}"
        icon = "⚠️"
    
    st.sidebar.markdown(
        f"""
        <div style="padding: 0.5rem; border-radius: 6px; margin-bottom: 1rem; font-weight: bold; text-align: center; transition: all 0.3s ease; {status_class}">
            {icon} {status_text}
        </div>
        """,
        unsafe_allow_html=True
    )

def create_liquid_progress_bar(progress, text="Processing..."):
    """Create animated liquid gradient progress bar with enhanced visual effects"""
    progress_html = f"""
    <div style="text-align: center; padding: 20px; background: linear-gradient(135deg, #2596be22, #ff7f0022); border-radius: 15px; border: 2px solid #2596be; margin: 20px 0;">
        <h3>🌊 {text}</h3>
        <div class="liquid-progress">
            <div class="liquid-fill" style="width: {progress}%"></div>
            <div class="progress-text">{progress:.1f}%</div>
        </div>
    </div>
    """
    return progress_html

def apply_custom_css():
    """Apply the aesthetic foundation with carefully crafted styles"""
    try:
        with open("assets/styles.css", "r") as css_file:
            st.markdown(f"<style>{css_file.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
        # Fallback inline CSS if external file not found
        st.markdown("""
        <style>
        .liquid-progress {
            width: 100%;
            height: 40px;
            background: linear-gradient(45deg, #1a1a1a, #2a2a2a);
            border-radius: 20px;
            overflow: hidden;
            position: relative;
            box-shadow: inset 0 2px 10px rgba(0,0,0,0.3);
        }
        
        .liquid-fill {
            height: 100%;
            background: linear-gradient(45deg, #2596be, #ff7f00, #2596be, #ff7f00);
            background-size: 400% 400%;
            animation: liquid-flow 3s ease-in-out infinite;
            border-radius: 20px;
            position: relative;
            overflow: hidden;
            transition: width 0.8s cubic-bezier(0.4, 0, 0.2, 1);
        }
        
        @keyframes liquid-flow {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        
        .progress-text {
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            color: white;
            font-weight: bold;
            text-shadow: 1px 1px 2px rgba(0,0,0,0.7);
            z-index: 10;
        }
        </style>
        """, unsafe_allow_html=True)

def create_client_header():
    """Compose the client interface header with institutional elegance"""
    client_id = st.session_state.get('client_id', st.session_state.get('current_client_id', 'Unknown'))
    username = st.session_state.get('username', 'User')
    
    st.markdown(f"""
        <div style="
            text-align: center;
            padding: 2rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 15px;
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        ">
            <h1>Enhanced Multi-Client Data Mapping System</h1>
            <p><em>Harmonizing Data Processing with Elegant Precision</em></p>
            <p>Welcome, <strong>{username}</strong> | Active Client: <strong>{client_id}</strong></p>
        </div>
    """, unsafe_allow_html=True)

def sidebar_controls():
    """Enhanced sidebar with all controls and database integration"""
    st.sidebar.header("📁 File Upload & Database")
    
    # Database status section
    st.sidebar.markdown("### 🗄️ Database Connection")
    database_status_widget()
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔍 Test Connection", use_container_width=True):
            with st.spinner("Testing database connection..."):
                check_database_connection()
            st.rerun()
    
    with col2:
        if st.button("📊 Load from DB", use_container_width=True):
            if st.session_state.current_client_id:
                try:
                    with st.spinner(f"Loading data for client {st.session_state.current_client_id}..."):
                        db_data = load_client_processed_data(st.session_state.current_client_id)
                        if db_data is not None and len(db_data) > 0:
                            st.session_state.processed_data = db_data
                            st.session_state.total_rows = len(db_data)
                            st.session_state.db_connection_status = "connected"
                            st.sidebar.success(f"✅ Loaded {len(db_data)} records")
                            st.rerun()
                        else:
                            st.sidebar.warning(f"⚠️ No data found for client {st.session_state.current_client_id}")
                except Exception as e:
                    st.sidebar.error(f"❌ Error loading from database: {str(e)}")
            else:
                st.sidebar.error("❌ Please select a client first")
    
    st.sidebar.divider()
    
    # Client selection with enhanced client loading
    st.sidebar.header("🏢 Client Management")
    
    available_clients = get_available_clients()
    st.session_state.available_clients = available_clients
    
    if available_clients:
        client_options = ["-- Select Client --"] + available_clients
        current_index = 0
        
        if st.session_state.current_client_id in available_clients:
            current_index = available_clients.index(st.session_state.current_client_id) + 1
        
        selected_client = st.sidebar.selectbox(
            "Select Client:",
            client_options,
            index=current_index,
            key="client_selector"
        )
        
        if selected_client != "-- Select Client --":
            if st.session_state.current_client_id != selected_client:
                st.session_state.current_client_id = selected_client
                st.session_state.processed_data = None
                st.session_state.form_data = {}
                st.rerun()
        else:
            st.session_state.current_client_id = None
    
    # Client management buttons
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        if st.button("➕ New Client", use_container_width=True):
            st.session_state.show_client_setup = True
            st.rerun()
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True):
            available_clients = get_available_clients()
            st.session_state.available_clients = available_clients
            st.rerun()
    
    # Display current client status
    if st.session_state.current_client_id:
        st.sidebar.success(f"📋 Active: **{st.session_state.current_client_id}**")
    else:
        st.sidebar.info("Please select a client first")
        return "", 1, 100, "None", ""
    
    st.sidebar.divider()
    
    # File upload section
    st.sidebar.markdown("**Required Files:**")
    file1 = st.sidebar.file_uploader("📄 Main TSV File", type=['tsv'], key="file1")
    file2 = st.sidebar.file_uploader("📊 Catalog TSV File", type=['tsv'], key="file2")
    dictionary = st.sidebar.file_uploader("📝 Dictionary JSON", type=['json'], key="dict")
    
    if file1 and file2 and dictionary:
        if st.sidebar.button("🚀 Process Files", type="primary", use_container_width=True):
            try:
                df1 = pd.read_csv(file1, delimiter="\t", dtype=str)
                df2 = pd.read_csv(file2, delimiter="\t", dtype=str)
                dict_data = json.load(dictionary)
                
                progress_container = st.empty()
                
                def progress_callback(progress_pct, message):
                    progress_container.markdown(
                        create_liquid_progress_bar(progress_pct, message), 
                        unsafe_allow_html=True
                    )
                
                result_df = process_files(df1, df2, dict_data, progress_callback, st.session_state.current_client_id)
                st.session_state.processed_data = result_df
                st.session_state.total_rows = len(result_df)
                
                output = BytesIO()
                result_df.to_csv(output, sep=";", index=False, encoding="utf-8")
                save_output_to_disk(output)
                
                progress_container.empty()
                st.sidebar.success("✅ Files processed successfully!")
                st.rerun()
                
            except Exception as e:
                st.sidebar.error(f"❌ Error: {str(e)}")
    
    # Filter controls with persistent state
    st.sidebar.divider()
    st.sidebar.header("🎯 Filters & Search")

    # Search text
    search_text = st.sidebar.text_input(
        "🔍 Search",
        value=st.session_state.get("search_text", ""),
        placeholder="Search in all columns..."
    )
    st.session_state.search_text = search_text

    # Similarity slider
    similarity_range = st.sidebar.slider(
        "Similarity %",
        min_value=1,
        max_value=100,
        value=st.session_state.get("similarity_range", (1, 100))
    )
    st.session_state.similarity_range = similarity_range

    # Filter column
    filter_column = st.sidebar.selectbox(
        "Filter Column",
        ["None", "Categoria", "Variedad", "Color", "Grado", "Catalog ID"],
        index=st.session_state.get("filter_column_index", 0)
    )
    st.session_state.filter_column = filter_column
    st.session_state.filter_column_index = ["None", "Categoria", "Variedad", "Color", "Grado", "Catalog ID"].index(filter_column)

    # Filter value
    filter_value = ""
    if filter_column != "None":
        filter_value = st.sidebar.text_input(
            "Filter Value",
            value=st.session_state.get("filter_value", ""),
            placeholder="Value to exclude..."
        )
        st.session_state.filter_value = filter_value
    else:
        st.session_state.filter_value = ""
    
    # Logout
    st.sidebar.divider()
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.switch_page("login.py")
    
    return search_text, similarity_range[0], similarity_range[1], filter_column, filter_value

def safe_float_conversion(series, default=0):
    """Safely convert a pandas Series to float, replacing errors with a default value."""
    def try_float(val):
        try:
            return float(str(val).replace('%', '').strip())
        except (ValueError, TypeError):
            return default
    return series.apply(try_float)

def apply_filters(df, search_text, min_sim, max_sim, filter_column, filter_value):
    """Apply filters to the dataframe with enhanced sorting and safe numeric conversion"""
    if df is None or len(df) == 0:
        return df
    
    filtered_df = df.copy()
    
    # Similarity filter with safe conversion
    if "Similarity %" in filtered_df.columns:
        filtered_df["Similarity %"] = safe_float_conversion(filtered_df["Similarity %"], 0)
        filtered_df = filtered_df[
            (filtered_df["Similarity %"] >= min_sim) & 
            (filtered_df["Similarity %"] <= max_sim)
        ]
        
        # Enhanced sorting: first by Similarity % descending, then by Vendor Product Description
        sort_columns = ["Similarity %"]
        sort_ascending = [False]
        
        if "Vendor Product Description" in filtered_df.columns:
            sort_columns.append("Vendor Product Description")
            sort_ascending.append(False)
        elif len(filtered_df.columns) > 0:
            sort_columns.append(filtered_df.columns[0])
            sort_ascending.append(False)
        
        filtered_df = filtered_df.sort_values(by=sort_columns, ascending=sort_ascending)
    
    # Search filter
    if search_text:
        mask = filtered_df.astype(str).apply(
            lambda row: search_text.lower() in row.to_string().lower(), axis=1
        )
        filtered_df = filtered_df[mask]
    
    # Column filter
    if filter_column != "None" and filter_value:
        if filter_column in filtered_df.columns:
            mask = ~filtered_df[filter_column].astype(str).str.contains(
                filter_value, case=False, na=False
            )
            filtered_df = filtered_df[mask]
    
    return filtered_df

def create_client_setup_modal():
    """Compose the new client creation modal with elegant precision"""
    if not st.session_state.show_client_setup:
        return
    
    st.markdown("""
        <div style="background: linear-gradient(135deg, #e3f2fd, #f1f8e9); border: 3px solid #2196f3; border-radius: 15px; padding: 30px; margin: 25px 0;">
            <h3 style="text-align: center; color: #1976d2; margin-bottom: 25px;">🏢 Create Enhanced Client Database System</h3>
        </div>
        """, unsafe_allow_html=True)
    
    new_client_id = st.text_input(
        "Client ID:",
        value=st.session_state.new_client_id,
        placeholder="e.g., acme_corp, client_001, demo_company"
    )
    st.session_state.new_client_id = new_client_id
    
    if new_client_id:
        valid_id = len(new_client_id) >= 3 and new_client_id.replace('_', '').replace('-', '').isalnum()
        
        if valid_id:
            st.info(f"Will create enhanced database system for: **{new_client_id}**")
        else:
            st.warning("Client ID must be at least 3 characters and contain only letters, numbers, hyphens, and underscores")
    else:
        valid_id = False
    
    col1, col2, col3 = st.columns([1, 1, 1])
    
    with col1:
        if st.button("🚀 Create Client", type="primary", use_container_width=True, disabled=not valid_id):
            if valid_id:
                with st.spinner(f"Creating enhanced database system for '{new_client_id}'..."):
                    try:
                        success, message = create_enhanced_client_databases(new_client_id)
                        
                        if success:
                            st.success(f"✅ {message}")
                            st.session_state.current_client_id = new_client_id
                            st.session_state.new_client_id = ''
                            st.session_state.show_client_setup = False
                            st.session_state.available_clients = get_available_clients()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"❌ {message}")
                    except Exception as e:
                        st.error(f"❌ Error creating client: {str(e)}")
    
    with col3:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state.show_client_setup = False
            st.session_state.new_client_id = ''
            st.rerun()

def create_edit_modal():
    """Create modal for editing category, variety, color, grade fields with enhanced functionality"""
    if st.session_state.show_edit_product_modal and st.session_state.edit_product_row_data is not None:
        st.markdown(
            """
            <div style="background: linear-gradient(135deg, #f8f9fa, #e9ecef); border: 2px solid #007bff; border-radius: 15px; padding: 25px; margin: 20px 0; box-shadow: 0 8px 25px rgba(0, 123, 255, 0.15);">
                <h3 style="text-align: center; color: #007bff; margin-bottom: 20px;">✏️ Edit Row Data</h3>
            </div>
            """,
            unsafe_allow_html=True
        )
        
        row_data = st.session_state.edit_product_row_data
        row_index = st.session_state.edit_product_row_index
        
        # Show current row information
        product_desc = str(row_data.get('Vendor Product Description', 'N/A'))
        vendor_name = str(row_data.get('Vendor Name', 'N/A'))
        similarity = row_data.get('Similarity %', 'N/A')
        missing = row_data.get('Missing Words', '')
        
        st.info(f"""
        **Editing Row {row_index}**
        
        **Product:** {product_desc[:100]}{'...' if len(product_desc) > 100 else ''}  
        **Vendor:** {vendor_name}  
        **Similarity:** {similarity}%
        **Missing Words:** {missing}
        """)
        
        # Re-run fuzzy matching button
        if st.button("🔄 Re-run Fuzzy Match", use_container_width=True):
            if st.session_state.current_client_id:
                with st.spinner("Re-processing row..."):
                    try:
                        success, updated_row = enhanced_reprocess_row(st.session_state.current_client_id, row_data, True)
                        
                        if success:
                            st.session_state.edit_product_row_data = updated_row
                            if st.session_state.processed_data is not None:
                                df = st.session_state.processed_data
                                if row_index in df.index:
                                    for key, value in updated_row.items():
                                        if key in df.columns:
                                            df.loc[row_index, key] = value
                            st.success(f"✅ Row re-processed! New similarity: {updated_row.get('Similarity %', 'N/A')}%")
                            st.rerun()
                        else:
                            st.error("❌ Failed to re-process row")
                    except Exception as e:
                        st.error(f"❌ Error re-processing row: {str(e)}")
            else:
                st.error("❌ No client selected")
        
        # Edit form in columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🏷️ Product Classification**")
            
            current_categoria = st.session_state.edit_categoria or str(row_data.get('Categoria', ''))
            current_variedad = st.session_state.edit_variedad or str(row_data.get('Variedad', ''))
            
            categoria = st.text_input(
                "Category:",
                value=current_categoria,
                key="modal_categoria_input"
            )
            
            variedad = st.text_input(
                "Variety:",
                value=current_variedad,
                key="modal_variedad_input"
            )
        
        with col2:
            st.markdown("**🎨 Physical Attributes**")
            
            current_color = st.session_state.edit_color or str(row_data.get('Color', ''))
            current_grado = st.session_state.edit_grado or str(row_data.get('Grado', ''))
            
            color = st.text_input(
                "Color:",
                value=current_color,
                key="modal_color_input"
            )
            
            grado = st.text_input(
                "Grade:",
                value=current_grado,
                key="modal_grado_input"
            )
        
        # Update session state with current values
        st.session_state.edit_categoria = categoria
        st.session_state.edit_variedad = variedad
        st.session_state.edit_color = color
        st.session_state.edit_grado = grado
        
        st.markdown("---")
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("💾 Update Row", type="primary", use_container_width=True):
                # Update the main dataframe
                if st.session_state.processed_data is not None:
                    df = st.session_state.processed_data
                    if row_index in df.index:
                        df.loc[row_index, 'Categoria'] = categoria
                        df.loc[row_index, 'Variedad'] = variedad
                        df.loc[row_index, 'Color'] = color
                        df.loc[row_index, 'Grado'] = grado
                
                # Update database if possible
                if st.session_state.current_client_id:
                    try:
                        success, message = enhanced_update_row_in_database(st.session_state.current_client_id, row_index, {
                            'Categoria': categoria, 'Variedad': variedad, 'Color': color, 'Grado': grado
                        })
                        
                        if success:
                            st.success(f"✅ Database updated successfully")
                        else:
                            st.warning(f"⚠️ Local update successful, database update failed: {message}")
                    except Exception as e:
                        st.warning(f"⚠️ Local update successful, database update failed: {str(e)}")
                
                # Close modal
                st.session_state.show_edit_product_modal = False
                st.session_state.edit_product_row_data = None
                st.session_state.edit_product_row_index = None
                st.session_state.edit_categoria = ''
                st.session_state.edit_variedad = ''
                st.session_state.edit_color = ''
                st.session_state.edit_grado = ''
                
                time.sleep(1)
                st.rerun()
        
        with col2:
            if st.button("🆕 Save New Product", use_container_width=True):
                if st.session_state.current_client_id:
                    try:
                        success, message = enhanced_save_staging_product(
                            st.session_state.current_client_id, row_data, categoria, variedad, color, grado, "streamlit_user"
                        )
                        
                        if success:
                            st.success(f"✅ {message}")
                        else:
                            st.error(f"❌ {message}")
                    except Exception as e:
                        st.error(f"❌ Error saving new product: {str(e)}")
                
                time.sleep(1)
                st.rerun()
        
        with col3:
            if st.button("🔄 Reset", use_container_width=True):
                st.session_state.edit_categoria = str(row_data.get('Categoria', ''))
                st.session_state.edit_variedad = str(row_data.get('Variedad', ''))
                st.session_state.edit_color = str(row_data.get('Color', ''))
                st.session_state.edit_grado = str(row_data.get('Grado', ''))
                st.rerun()
        
        with col4:
            if st.button("❌ Cancel", use_container_width=True):
                st.session_state.show_edit_product_modal = False
                st.session_state.edit_product_row_data = None
                st.session_state.edit_product_row_index = None
                st.session_state.edit_categoria = ''
                st.session_state.edit_variedad = ''
                st.session_state.edit_color = ''
                st.session_state.edit_grado = ''
                st.rerun()

def create_streamlit_table_with_actions(df):
    """Create table using Streamlit native components with inline action buttons"""
    
    # Key columns for display
    display_cols = [
        'Cleaned input', 'Best match', 'Similarity %', 'Catalog ID', 
        'Categoria', 'Variedad', 'Color', 'Grado'
    ]
    
    # Filter to only show columns that exist in the dataframe
    display_cols = [col for col in display_cols if col in df.columns]
    
    # Create header
    header_cols = st.columns(len(display_cols) + 4)  # +4 for Accept, Deny, Actions, Status
    
    headers = ["🧹 Cleaned Input", "🎯 Best Match", "📊 Similarity %", "🏷️ Catalog ID", 
               "📂 Category", "🌿 Variety", "🎨 Color", "⭐ Grade", 
               "✅ Accept", "❌ Deny", "⚡ Actions", "📊 Status"]
    
    for i, header in enumerate(headers):
        with header_cols[i]:
            st.markdown(f"**{header}**")
    
    # Add separator
    st.markdown("---")
    
    # Iterate through each row and create the table
    for idx, row in df.iterrows():
        # Create columns for this row
        row_cols = st.columns(len(display_cols) + 4)
        
        # Display data columns
        with row_cols[0]:
            cleaned_input = str(row.get('Cleaned input', ''))
            st.markdown(f"<div style='background: linear-gradient(135deg, #fffec6, #fff9c4); border: 1px solid #ffc107; border-radius: 6px; padding: 8px; font-weight: bold;'>{cleaned_input[:50]}{'...' if len(cleaned_input) > 50 else ''}</div>", unsafe_allow_html=True)
        
        with row_cols[1]:
            best_match = str(row.get('Best match', ''))
            st.markdown(f"<div style='background: linear-gradient(135deg, #fffec6, #fff9c4); border: 1px solid #ffc107; border-radius: 6px; padding: 8px; font-weight: bold;'>{best_match[:50]}{'...' if len(best_match) > 50 else ''}</div>", unsafe_allow_html=True)
        
        with row_cols[2]:
            try:
                similarity = float(str(row.get('Similarity %', 0)).replace('%', ''))
            except (ValueError, TypeError):
                similarity = 0
                
            if similarity >= 90:
                color_class = "background-color: #d4edda; color: #155724;"
            elif similarity >= 70:
                color_class = "background-color: #fff3cd; color: #856404;"
            else:
                color_class = "background-color: #f8d7da; color: #721c24;"
            st.markdown(f"<div style='{color_class} padding: 8px; border-radius: 4px; text-align: center; font-weight: bold;'>{similarity}%</div>", unsafe_allow_html=True)
        
        with row_cols[3]:
            catalog_id = str(row.get('Catalog ID', ''))
            if catalog_id.strip() in ["111111.0", "111111"]:
                st.markdown("<div style='background-color: #ff7f00; color: white; padding: 8px; border-radius: 4px; text-align: center; font-weight: bold;'>needs to create product</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div style='background: linear-gradient(135deg, #fffec6, #fff9c4); border: 1px solid #ffc107; border-radius: 6px; padding: 8px; font-weight: bold;'>{catalog_id}</div>", unsafe_allow_html=True)
        
        with row_cols[4]:
            st.text(str(row.get('Categoria', '')))
        
        with row_cols[5]:
            st.text(str(row.get('Variedad', '')))
        
        with row_cols[6]:
            st.text(str(row.get('Color', '')))
        
        with row_cols[7]:
            st.text(str(row.get('Grado', '')))
                
        # Accept checkbox
        with row_cols[8]:
            accept_key = f"accept_{idx}"
            db_accept = str(row.get("Accept Map", "")).strip().lower() == "true"
            accept = st.session_state.form_data.get(accept_key, db_accept)
            new_accept = st.checkbox("", value=accept, key=f"accept_cb_inline_{idx}")
            st.session_state.form_data[accept_key] = new_accept

            # Exclusivity: if accept is marked, unmark deny
            if new_accept and st.session_state.form_data.get(f"deny_{idx}", False):
                st.session_state.form_data[f"deny_{idx}"] = False
        
        # Deny checkbox
        with row_cols[9]:
            deny_key = f"deny_{idx}"
            db_deny = str(row.get("Deny Map", "")).strip().lower() == "true"
            deny = st.session_state.form_data.get(deny_key, db_deny)
            new_deny = st.checkbox("", value=deny, key=f"deny_cb_inline_{idx}")
            st.session_state.form_data[deny_key] = new_deny

            # Exclusivity: if deny is marked, unmark accept
            if new_deny and st.session_state.form_data.get(f"accept_{idx}", False):
                st.session_state.form_data[f"accept_{idx}"] = False
        
        # Action buttons
        with row_cols[10]:
            action_col1, action_col2 = st.columns(2)
            
            with action_col1:
                if st.button("✏️", key=f"edit_inline_{idx}", help="Edit and Verify"):
                    st.session_state.show_edit_product_modal = True
                    st.session_state.edit_product_row_data = row.to_dict()
                    st.session_state.edit_product_row_index = idx
                    
                    # Initialize edit values
                    st.session_state.edit_categoria = str(row.get('Categoria', ''))
                    st.session_state.edit_variedad = str(row.get('Variedad', ''))
                    st.session_state.edit_color = str(row.get('Color', ''))
                    st.session_state.edit_grado = str(row.get('Grado', ''))
                    
                    st.rerun()
            
            with action_col2:
                if st.button("🔄", key=f"reprocess_inline_{idx}", help="Re-run fuzzy matching"):
                    if st.session_state.current_client_id:
                        with st.spinner("Re-processing..."):
                            try:
                                success, updated_row = enhanced_reprocess_row(st.session_state.current_client_id, row.to_dict(), True)
                                
                                if success:
                                    # Update the main dataframe
                                    if st.session_state.processed_data is not None:
                                        for key, value in updated_row.items():
                                            if key in st.session_state.processed_data.columns:
                                                st.session_state.processed_data.loc[idx, key] = value
                                    
                                    st.success(f"✅ Row {idx} re-processed successfully!")
                                    st.rerun()
                                else:
                                    st.error(f"❌ Failed to re-process row {idx}")
                            except Exception as e:
                                st.error(f"❌ Error re-processing row {idx}: {str(e)}")
                    else:
                        st.error("❌ No client selected")
        
        # Status indicators
        with row_cols[11]:
            if st.session_state.form_data.get(f"accept_{idx}", False):
                st.markdown("✅")
            elif st.session_state.form_data.get(f"deny_{idx}", False):
                st.markdown("❌")
            else:
                st.markdown("⏳")
        
        # Add a subtle separator between rows
        st.markdown("<hr style='margin: 10px 0; border: 1px solid #eee;'>", unsafe_allow_html=True)
    
    # Database Operations
    st.markdown("---")
    st.markdown("### 💾 Database Operations")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("💾 Save All to Database", type="primary", use_container_width=True):
            if st.session_state.processed_data is not None and st.session_state.current_client_id:
                with st.spinner("Saving to database..."):
                    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    success, message = save_client_processed_data(
                        st.session_state.current_client_id, 
                        st.session_state.processed_data, 
                        batch_id
                    )
                    
                    if success:
                        st.success(f"✅ {message}")
                    else:
                        st.error(f"❌ {message}")
            else:
                st.error("❌ No data to save or no client selected")

    with col2:
        if st.button("🔄 Reload from Database", use_container_width=True):
            if st.session_state.current_client_id:
                with st.spinner("Reloading from database..."):
                    db_data = load_client_processed_data(st.session_state.current_client_id)
                    if db_data is not None:
                        st.session_state.processed_data = db_data
                        st.session_state.total_rows = len(db_data)
                        st.success(f"✅ Reloaded {len(db_data)} records")
                        st.rerun()
                    else:
                        st.warning("⚠️ No data found in database")
            else:
                st.error("❌ No client selected")

    with col3:
        if st.button("📊 Show Client Stats", use_container_width=True):
            if st.session_state.current_client_id:
                try:
                    stats = get_client_statistics(st.session_state.current_client_id)
                    
                    if 'error' not in stats:
                        st.json(stats['main_stats'])
                    else:
                        st.error(f"❌ {stats['error']}")
                except Exception as e:
                    st.error(f"❌ Error getting stats: {str(e)}")
            else:
                st.error("❌ No client selected")

def mark_all_accept(filtered_df):
    """Mark all visible rows as Accept and clear Deny"""
    for idx in filtered_df.index:
        st.session_state.form_data[f"accept_{idx}"] = True
        st.session_state.form_data[f"deny_{idx}"] = False

def mark_all_deny(filtered_df):
    """Mark all visible rows as Deny and clear Accept"""
    for idx in filtered_df.index:
        st.session_state.form_data[f"accept_{idx}"] = False
        st.session_state.form_data[f"deny_{idx}"] = True

def display_messages():
    """Display status messages with elegant presentation"""
    if st.session_state.get('success_message'):
        st.success(st.session_state.success_message)
        st.session_state.success_message = ''
    
    if st.session_state.get('error_message'):
        st.error(st.session_state.error_message)
        st.session_state.error_message = ''
    
    if st.session_state.get('info_message'):
        st.info(st.session_state.info_message)
        st.session_state.info_message = ''
    
    if st.session_state.get('warning_message'):
        st.warning(st.session_state.warning_message)
        st.session_state.warning_message = ''

def data_mapping_tab():
    """Main data mapping functionality tab"""
    # Sidebar controls
    search_text, min_sim, max_sim, filter_column, filter_value = sidebar_controls()
    
    # Main content
    if st.session_state.processed_data is not None:
        df = st.session_state.processed_data
        
        # Apply filters
        filtered_df = apply_filters(df, search_text, min_sim, max_sim, filter_column, filter_value)
        
        if len(filtered_df) > 0:
            # Progress tracking
            total_rows = len(filtered_df)
            reviewed_rows = sum(
                1 for idx in filtered_df.index 
                if st.session_state.form_data.get(f"accept_{idx}", False) or 
                   st.session_state.form_data.get(f"deny_{idx}", False)
            )
            
            progress_pct = (reviewed_rows / total_rows * 100) if total_rows > 0 else 0
            
            # Progress display with liquid effects
            st.markdown(
                f"""
                <div style="background: #f8f9fa; padding: 1rem; border-radius: 8px; margin-bottom: 1rem; border-left: 4px solid #28a745;">
                    <h4>📊 Review Progress</h4>
                    <p><strong>{reviewed_rows}</strong> of <strong>{total_rows}</strong> rows reviewed 
                    (<strong>{progress_pct:.1f}%</strong>)</p>
                    <small>Data sorted by: Similarity % ↓, Vendor Product Description ↓</small>
                </div>
                """, 
                unsafe_allow_html=True
            )
            
            # Liquid progress bar
            progress_html = create_liquid_progress_bar(progress_pct, f"Review Progress: {reviewed_rows}/{total_rows}")
            st.markdown(progress_html, unsafe_allow_html=True)
            
            # Pagination
            rows_per_page = 50
            total_pages = (len(filtered_df) + rows_per_page - 1) // rows_per_page
            
            if total_pages > 1:
                st.write(f"**Total rows:** {len(filtered_df)} | **Pages:** {total_pages}")
                
                current_page = st.selectbox(
                    f"Select Page (1-{total_pages})", 
                    options=list(range(1, total_pages + 1)),
                    index=st.session_state.current_page - 1,
                    key="page_selector_main"
                )
                
                st.session_state.current_page = current_page
                
                start_idx = (current_page - 1) * rows_per_page
                end_idx = start_idx + rows_per_page
                page_df = filtered_df.iloc[start_idx:end_idx].copy()
            else:
                page_df = filtered_df.copy()
                st.session_state.current_page = 1
            
            # Create the enhanced inline table
            create_streamlit_table_with_actions(page_df)
            
            # Add bulk action buttons at the bottom
            st.markdown("---")
            st.markdown(
                """
                <div style="background: linear-gradient(135deg, #f1f3f4, #e8eaed); border: 2px solid #1976d2; border-radius: 12px; padding: 20px; margin: 20px 0;">
                    <div style="text-align: center; color: #1976d2; font-weight: bold; margin-bottom: 15px; font-size: 1.1em;">⚡ Bulk Actions for Current Page</div>
                </div>
                """,
                unsafe_allow_html=True
            )
            
            col1, col2, col3 = st.columns([1, 1, 1])
            
            with col1:
                if st.button("✅ Accept All Visible", 
                             type="primary", 
                             use_container_width=True, 
                             key="bulk_accept_btn"):
                    mark_all_accept(page_df)
                    st.success(f"✅ Marked {len(page_df)} rows as Accept")
                    st.rerun()
            
            with col2:
                if st.button("❌ Deny All Visible", 
                             use_container_width=True, 
                             key="bulk_deny_btn"):
                    mark_all_deny(page_df)
                    st.success(f"❌ Marked {len(page_df)} rows as Deny")
                    st.rerun()
            
            with col3:
                if st.button("🔄 Clear All Selections", 
                             use_container_width=True, 
                             key="bulk_clear_btn"):
                    for idx in page_df.index:
                        st.session_state.form_data[f"accept_{idx}"] = False
                        st.session_state.form_data[f"deny_{idx}"] = False
                    st.info(f"🔄 Cleared all selections for {len(page_df)} rows")
                    st.rerun()
            
        else:
            st.warning("🔍 No data matches the current filters")
            st.info("Try adjusting your filter criteria in the sidebar")
    else:
        # Instructions for new users
        st.info("👆 **Get Started:** Upload your files using the sidebar or load existing data from database")

def staging_products_tab():
    """Staging Products to Create tab"""
    st.header("🆕 Staging Products to Create")
    
    if st.session_state.current_client_id:
        try:
            # Load staging products (mock implementation)
            st.info("Staging products functionality - coming soon in full implementation")
            
            # Show example of what staging products would look like
            if st.session_state.processed_data is not None:
                staging_products = st.session_state.processed_data[
                    st.session_state.processed_data["Catalog ID"].astype(str).str.contains("111111", na=False)
                ]
                
                if len(staging_products) > 0:
                    st.write(f"**Products marked for creation:** {len(staging_products)}")
                    st.dataframe(staging_products[['Cleaned input', 'Categoria', 'Variedad', 'Color', 'Grado']])
                else:
                    st.info("No products currently marked for staging")
            else:
                st.info("Process some data first to see staging products")
                
        except Exception as e:
            st.error(f"❌ Error loading staging products: {str(e)}")
    else:
        st.warning("⚠️ Please select a client to view staging products")

def synonyms_blacklist_tab():
    """Synonyms & Blacklist Management tab"""
    st.header("📝 Synonyms & Blacklist Management")
    
    if st.session_state.current_client_id:
        try:
            # Load current data
            current_data = get_client_synonyms_blacklist(st.session_state.current_client_id)
            
            # Show statistics
            col1, col2 = st.columns(2)
            with col1:
                st.metric("🔁 Synonyms", len(current_data.get('synonyms', {})))
            with col2:
                st.metric("🛑 Blacklist", len(current_data.get('blacklist', {}).get('input', [])))
            
            # Create sub-tabs for view and edit
            view_tab, edit_tab = st.tabs(["👀 View Current", "✏️ Edit & Update"])
            
            with view_tab:
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("🔁 Current Synonyms")
                    if current_data['synonyms']:
                        for original, replacement in current_data['synonyms'].items():
                            st.write(f"• `{original}` → `{replacement}`")
                    else:
                        st.info("No synonyms configured for this client")
                
                with col2:
                    st.subheader("🛑 Current Blacklist")
                    if current_data['blacklist']['input']:
                        for word in current_data['blacklist']['input']:
                            st.write(f"• `{word}`")
                    else:
                        st.info("No blacklist words configured for this client")
            
            with edit_tab:
                st.subheader("➕ Add New Synonym")
                
                col1, col2, col3 = st.columns([2, 2, 1])
                with col1:
                    original_word = st.text_input("Original Word:", key="new_synonym_original")
                with col2:
                    replacement_word = st.text_input("Replacement Word:", key="new_synonym_replacement")
                with col3:
                    if st.button("➕ Add Synonym", key="add_synonym_btn"):
                        if original_word and replacement_word:
                            # Add to existing synonyms
                            new_synonyms = current_data['synonyms'].copy()
                            new_synonyms[original_word] = replacement_word
                            
                            # Convert to required format
                            synonyms_list = [{k: v} for k, v in new_synonyms.items()]
                            
                            success, message = update_client_synonyms_blacklist(
                                st.session_state.current_client_id,
                                synonyms_list,
                                current_data['blacklist']['input']
                            )
                            
                            if success:
                                st.success(f"✅ Synonym added: {original_word} → {replacement_word}")
                                st.rerun()
                            else:
                                st.error(f"❌ {message}")
                        else:
                            st.error("Please enter both original and replacement words")
                
                st.subheader("➕ Add New Blacklist Word")
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    blacklist_word = st.text_input("Blacklist Word:", key="new_blacklist_word")
                with col2:
                    if st.button("➕ Add Word", key="add_blacklist_btn"):
                        if blacklist_word:
                            # Add to existing blacklist
                            new_blacklist = current_data['blacklist']['input'].copy()
                            if blacklist_word not in new_blacklist:
                                new_blacklist.append(blacklist_word)
                                
                                # Convert synonyms to required format
                                synonyms_list = [{k: v} for k, v in current_data['synonyms'].items()]
                                
                                success, message = update_client_synonyms_blacklist(
                                    st.session_state.current_client_id,
                                    synonyms_list,
                                    new_blacklist
                                )
                                
                                if success:
                                    st.success(f"✅ Blacklist word added: {blacklist_word}")
                                    st.rerun()
                                else:
                                    st.error(f"❌ {message}")
                            else:
                                st.warning("Word already in blacklist")
                        else:
                            st.error("Please enter a word")
                
        except Exception as e:
            st.error(f"❌ Error managing synonyms/blacklist: {str(e)}")
    else:
        st.warning("⚠️ Please select a client to manage synonyms and blacklist")

def main():
    """
    The grand conductor of client application symphony,
    orchestrating all components into harmonious workflow
    """
    # Configure page with aesthetic precision
    st.set_page_config(
        page_title="Client Interface - Enhanced Multi-Client System",
        page_icon="🎯",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session management
    initialize_session_state()
    
    # Verify client authentication
    # check_client_authentication()
    
    # Apply aesthetic foundation
    apply_custom_css()
    
    # Initialize database connection status
    if st.session_state.db_connection_status is None:
        check_database_connection()
    
    # Show modals if needed
    if st.session_state.show_client_setup:
        create_client_setup_modal()
        return
    
    if st.session_state.show_edit_product_modal:
        create_edit_modal()
        return
    
    # Display application header
    create_client_header()
    
    # Display contextual messages
    display_messages()
    
    # Main application interface
    if st.session_state.current_client_id:
        tab1, tab2, tab3 = st.tabs(["📊 Data Mapping", "🆕 Staging Products", "📝 Synonyms & Blacklist"])
        
        with tab1:
            data_mapping_tab()
        
        with tab2:
            staging_products_tab()
        
        with tab3:
            synonyms_blacklist_tab()
    else:
        # Sidebar controls for client selection
        sidebar_controls()
        
        # Instructions for new users
        st.info("👆 **Get Started:** Select a client using the sidebar")
        
        with st.expander("📖 **Enhanced Features Guide**"):
            st.markdown("""
            ### 🚀 **Getting Started:**
            
            1. **Select Client** → Use sidebar to choose existing or create new client
            2. **Load Data** → Either upload files or load from database
            3. **Review Data** → Use enhanced filtering and row-level actions
            4. **Edit Products** → Click edit button to modify and save products
            5. **Bulk Actions** → Use bulk operations for efficient workflow
            
            ### 🔧 **Enhanced Features:**
            - ✅ **Row-level Processing** - Individual row reprocessing with updated synonyms
            - ✅ **Enhanced Filtering** - Advanced search and exclusion capabilities
            - ✅ **Product Staging** - Save new products with catalog_id: 111111
            - ✅ **Real-time Updates** - Immediate feedback and validation
            - ✅ **Client Isolation** - Complete data separation per client
            - ✅ **Liquid Progress Bars** - Beautiful animated progress tracking
            - ✅ **Database Integration** - Full CRUD operations with enhanced database
            """)

if __name__ == "__main__":
    main()