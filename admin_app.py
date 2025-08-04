# admin_app.py - Enhanced Multi-Client Admin Interface
"""
The conductor's podium of system administration - where client orchestration
meets database harmony through elegant controls and insightful analytics.
A streamlined gateway to multi-client management with poetic precision.
"""

import streamlit as st
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, List

# Import our backend systems
from backend import (
    get_available_clients,
    create_enhanced_client_databases,
    get_client_statistics,
    verify_client_database_structure,
    test_client_database_connection
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_admin_session():
    """Initialize the administrative session with elegant defaults"""
    session_vars = {
        'selected_client': None,
        'show_client_creation': False,
        'new_client_id': '',
        'refresh_trigger': 0
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value

def check_admin_authentication():
    """Verify administrative privileges with graceful redirection"""
    if not st.session_state.get('authenticated', False):
        st.error("Authentication required. Please login first.")
        if st.button("Return to Login"):
            st.switch_page("login.py")
        st.stop()
    
    if st.session_state.get('user_role') != 'admin':
        st.error("Administrative privileges required.")
        if st.button("Return to Login"):
            st.switch_page("login.py")
        st.stop()

def create_admin_header():
    """Compose the administrative header with institutional elegance"""
    st.markdown("""
        <div style="
            text-align: center;
            padding: 2rem;
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
            color: white;
            border-radius: 15px;
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        ">
            <h1>System Administration Dashboard</h1>
            <p><em>Multi-Client Database Management & Analytics</em></p>
            <p>Welcome, {}</p>
        </div>
    """.format(st.session_state.get('username', 'Administrator')), unsafe_allow_html=True)

def display_system_overview():
    """Present system overview with key metrics and health indicators"""
    st.subheader("System Overview")
    
    # Test database connection
    with st.spinner("Checking system health..."):
        db_status, db_message = test_client_database_connection()
    
    # Display connection status
    if db_status:
        st.success(f"Database Connection: {db_message}")
    else:
        st.error(f"Database Connection Failed: {db_message}")
        return
    
    # Get available clients
    clients = get_available_clients()
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Active Clients",
            value=len(clients),
            help="Number of client databases in the system"
        )
    
    with col2:
        st.metric(
            label="System Status",
            value="Operational" if db_status else "Warning",
            help="Overall system health indicator"
        )
    
    with col3:
        st.metric(
            label="Last Updated",
            value=datetime.now().strftime("%H:%M:%S"),
            help="Current timestamp"
        )

def create_client_management_section():
    """Orchestrate client management with creation and monitoring capabilities"""
    st.subheader("Client Management")
    
    # Get available clients
    clients = get_available_clients()
    
    if clients:
        st.write(f"**Managing {len(clients)} clients:**")
        
        # Client selection
        selected_client = st.selectbox(
            "Select Client for Management:",
            ["-- Select Client --"] + clients,
            key="admin_client_selector"
        )
        
        if selected_client != "-- Select Client --":
            st.session_state.selected_client = selected_client
            
            # Display client information
            with st.expander(f"Client Details: {selected_client}", expanded=True):
                display_client_details(selected_client)
    else:
        st.info("No clients found in the system.")
    
    # Client creation section
    st.markdown("---")
    st.subheader("Create New Client")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        new_client_id = st.text_input(
            "New Client ID:",
            value=st.session_state.new_client_id,
            placeholder="e.g., new_company, client_001",
            help="Client ID must be unique and contain only letters, numbers, hyphens, and underscores"
        )
        st.session_state.new_client_id = new_client_id
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Create Client", type="primary"):
            if new_client_id and len(new_client_id) >= 3:
                if new_client_id.replace('_', '').replace('-', '').isalnum():
                    create_new_client(new_client_id)
                else:
                    st.error("Invalid client ID format")
            else:
                st.error("Client ID must be at least 3 characters")

def display_client_details(client_id: str):
    """Display comprehensive client information with actionable insights"""
    try:
        # Verify database structure
        with st.spinner("Checking client database structure..."):
            structure_ok, structure_results = verify_client_database_structure(client_id)
        
        # Display structure status
        st.subheader("Database Structure Status")
        if structure_ok:
            st.success("All client databases verified successfully")
        else:
            st.warning("Database structure issues detected")
        
        # Show detailed results
        for db_name, status in structure_results.items():
            if "✅" in status:
                st.success(f"{db_name}: {status}")
            else:
                st.error(f"{db_name}: {status}")
        
        # Get client statistics
        st.subheader("Client Statistics")
        with st.spinner("Loading client statistics..."):
            stats = get_client_statistics(client_id)
        
        if 'error' not in stats:
            main_stats = stats.get('main_stats', {})
            
            # Display statistics metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Records", main_stats.get('total_records', 0))
            
            with col2:
                st.metric("Accepted Records", main_stats.get('accepted_records', 0))
            
            with col3:
                st.metric("Unique Vendors", main_stats.get('unique_vendors', 0))
            
            with col4:
                st.metric(
                    "Avg Similarity", 
                    f"{main_stats.get('avg_similarity', 0):.1f}%"
                )
            
            # Additional metrics
            if main_stats.get('total_records', 0) > 0:
                acceptance_rate = main_stats.get('acceptance_rate', 0)
                st.metric("Acceptance Rate", f"{acceptance_rate:.1f}%")
        else:
            st.error(f"Error loading statistics: {stats['error']}")
        
        # Action buttons
        st.subheader("Client Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button(f"Refresh {client_id}", key=f"refresh_{client_id}"):
                st.session_state.refresh_trigger += 1
                st.rerun()
        
        with col2:
            if st.button(f"View Data", key=f"view_{client_id}"):
                st.info("Data viewing interface - available in client_app.py")
        
        with col3:
            if st.button(f"Export Stats", key=f"export_{client_id}"):
                # Create downloadable statistics
                stats_df = pd.DataFrame([main_stats])
                csv_data = stats_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv_data,
                    file_name=f"client_stats_{client_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
    except Exception as e:
        st.error(f"Error loading client details: {str(e)}")

def create_new_client(client_id: str):
    """Create new client with comprehensive database structure"""
    with st.spinner(f"Creating enhanced database system for '{client_id}'..."):
        try:
            success, message = create_enhanced_client_databases(client_id)
            
            if success:
                st.success(f"Successfully created client: {client_id}")
                st.info(message)
                
                # Reset form
                st.session_state.new_client_id = ''
                
                # Refresh page to show new client
                st.session_state.refresh_trigger += 1
                st.rerun()
            else:
                st.error(f"Failed to create client: {message}")
                
        except Exception as e:
            st.error(f"Error creating client: {str(e)}")

def create_sidebar_navigation():
    """Compose elegant sidebar navigation with system controls"""
    st.sidebar.header("Administration Menu")
    
    # System information
    st.sidebar.subheader("System Information")
    st.sidebar.info(f"Logged in as: **{st.session_state.get('username', 'Admin')}**")
    st.sidebar.info(f"Role: **{st.session_state.get('user_role', 'admin').title()}**")
    
    # Navigation buttons
    st.sidebar.subheader("Navigation")
    
    if st.sidebar.button("Refresh System", use_container_width=True):
        st.session_state.refresh_trigger += 1
        st.rerun()
    
    if st.sidebar.button("Client Interface", use_container_width=True):
        st.info("Switch to client_app.py for data processing interface")
    
    if st.sidebar.button("Logout", use_container_width=True):
        # Clear session state
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.success("Logged out successfully!")
        st.switch_page("login.py")

def main():
    """
    The maestro function orchestrating the administrative symphony,
    conducting the harmonious flow of system management operations
    """
    # Configure page with administrative elegance
    st.set_page_config(
        page_title="Admin Dashboard - Enhanced Multi-Client System",
        page_icon="⚙️",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize administrative session
    initialize_admin_session()
    
    # Verify administrative access
    check_admin_authentication()
    
    # Apply refined styling
    st.markdown("""
        <style>
        .main { padding-top: 1rem; }
        .stButton > button { 
            border-radius: 8px; 
            font-weight: bold;
            transition: all 0.3s ease;
        }
        .stButton > button:hover { 
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        }
        .stMetric { 
            background: linear-gradient(135deg, #f8f9fa, #e9ecef);
            padding: 1rem;
            border-radius: 8px;
            border: 1px solid #dee2e6;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Create sidebar navigation
    create_sidebar_navigation()
    
    # Display administrative header
    create_admin_header()
    
    # Main administrative interface
    try:
        # System overview section
        display_system_overview()
        
        st.markdown("---")
        
        # Client management section
        create_client_management_section()
        
        # Footer with system information
        st.markdown("---")
        st.markdown(
            f"**System Status:** Database operational | "
            f"**Clients:** {len(get_available_clients())} | "
            f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
    except Exception as e:
        st.error(f"Administrative interface error: {str(e)}")
        logger.error(f"Admin interface error: {str(e)}")
        
        if st.button("Retry"):
            st.rerun()

if __name__ == "__main__":
    main()