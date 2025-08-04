# login.py - Authentication Gateway for Enhanced Multi-Client System
"""
The elegant entrance to our data mapping symphony - where credentials
transform into authorized access through secure validation and graceful
session management with role-based navigation orchestration.
"""

import streamlit as st
import pymysql
import bcrypt
import logging
from typing import Tuple, Optional

# Configure logging with precision
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root', 
    'password': 'Maracuya123',
    'database': 'mapping_validation_system',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

def initialize_session_state():
    """Initialize the foundation of session management"""
    session_vars = {
        'authenticated': False,
        'username': None,
        'user_role': None,
        'client_id': None,
        'login_attempts': 0,
        'show_password': False,
        'redirect_to': None
    }
    
    for var, default_value in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default_value

def authenticate_user(username: str, password: str) -> Tuple[bool, Optional[dict]]:
    """
    Authenticate user credentials against the database with elegant precision,
    returning success status and user information upon validation
    """
    try:
        connection = pymysql.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Query user credentials with security awareness
        cursor.execute("""
            SELECT id, username, password_hash, client_id, role, is_active
            FROM user_credentials 
            WHERE username = %s AND is_active = TRUE
        """, (username,))
        
        user_record = cursor.fetchone()
        
        if user_record:
            # Verify password with bcrypt elegance
            stored_hash = user_record['password_hash'].encode('utf-8')
            provided_password = password.encode('utf-8')
            
            if bcrypt.checkpw(provided_password, stored_hash):
                # Update last login timestamp
                cursor.execute("""
                    UPDATE user_credentials 
                    SET last_login = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (user_record['id'],))
                connection.commit()
                
                cursor.close()
                connection.close()
                
                logger.info(f"Successful authentication for user: {username}")
                return True, user_record
            else:
                logger.warning(f"Invalid password for user: {username}")
        else:
            logger.warning(f"User not found: {username}")
        
        cursor.close()
        connection.close()
        return False, None
        
    except Exception as e:
        logger.error(f"Authentication error: {str(e)}")
        return False, None

def create_admin_interface():
    """Display admin interface directly in login.py when authenticated as admin"""
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
            <p><em>Multi-Client Database Management</em></p>
            <p>Welcome, {}</p>
        </div>
    """.format(st.session_state.get('username', 'Administrator')), unsafe_allow_html=True)
    
    # Import admin functions here to avoid circular imports
    try:
        from backend import (
            get_available_clients,
            create_enhanced_client_databases,
            test_client_database_connection
        )
        
        # Test database connection
        with st.spinner("Checking system health..."):
            db_status, db_message = test_client_database_connection()
        
        if db_status:
            st.success(f"Database Connection: {db_message}")
        else:
            st.error(f"Database Connection Failed: {db_message}")
        
        # Get available clients
        clients = get_available_clients()
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Active Clients", len(clients))
        
        with col2:
            st.metric("System Status", "Operational" if db_status else "Warning")
        
        with col3:
            if st.button("Refresh System"):
                st.rerun()
        
        with col4:
            if st.button("Logout"):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
        
        st.markdown("---")
        
        # Client management
        st.subheader("Client Management")
        
        if clients:
            st.write(f"**Active Clients ({len(clients)}):**")
            for client in clients:
                st.write(f"• {client}")
        else:
            st.info("No clients found in the system.")
        
        # Create new client
        st.subheader("Create New Client")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            new_client_id = st.text_input(
                "New Client ID:",
                placeholder="e.g., new_company, client_001",
                help="Client ID must be unique and contain only letters, numbers, hyphens, and underscores"
            )
        
        with col2:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Create Client", type="primary"):
                if new_client_id and len(new_client_id) >= 3:
                    if new_client_id.replace('_', '').replace('-', '').isalnum():
                        with st.spinner(f"Creating client '{new_client_id}'..."):
                            try:
                                success, message = create_enhanced_client_databases(new_client_id)
                                if success:
                                    st.success(f"Successfully created client: {new_client_id}")
                                    st.rerun()
                                else:
                                    st.error(f"Failed to create client: {message}")
                            except Exception as e:
                                st.error(f"Error creating client: {str(e)}")
                    else:
                        st.error("Invalid client ID format")
                else:
                    st.error("Client ID must be at least 3 characters")
                    
        # Basic admin instructions
        st.markdown("---")
        st.info("""
        **Next Steps:**
        1. Create a test client using the form above
        2. Once you have the full system running, you can access `client_app.py` for data processing
        3. Use this interface to monitor and manage your clients
        """)
        
    except ImportError as e:
        st.error(f"Backend import error: {str(e)}")
        st.info("Make sure all backend files are in the same directory")

def create_client_interface():
    """Display client interface directly in login.py when authenticated as client"""
    st.markdown("""
        <div style="
            text-align: center;
            padding: 2rem;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 15px;
            margin-bottom: 2rem;
            box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        ">
            <h1>Client Data Processing Interface</h1>
            <p><em>Enhanced Multi-Client Data Mapping</em></p>
            <p>Welcome, {}</p>
        </div>
    """.format(st.session_state.get('username', 'User')), unsafe_allow_html=True)
    
    # Display client information
    client_id = st.session_state.get('client_id')
    if client_id:
        st.success(f"Active Client: **{client_id}**")
    else:
        st.warning("No client ID associated with this account")
    
    # Basic client functionality placeholder
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("**Data Processing**\nUpload and process your TSV files here")
    
    with col2:
        st.info("**Review Results**\nReview and validate mapping results")
    
    with col3:
        st.info("**Export Data**\nDownload processed results")
    
    # Logout button
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Logout", type="primary", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    
    st.markdown("---")
    st.info("""
    **Note:** This is a basic client interface. The full `client_app.py` with complete 
    data processing functionality will be available once the system is fully deployed.
    """)

def create_login_interface():
    """
    Compose the login interface with aesthetic refinement and functional elegance,
    orchestrating the dance between user input and system validation
    """
    # Create centered container with poetic spacing
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        # Application header with gradient elegance
        st.markdown("""
            <div style="
                text-align: center;
                padding: 2rem;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                border-radius: 15px;
                margin-bottom: 2rem;
                box-shadow: 0 8px 32px rgba(0,0,0,0.1);
            ">
                <h1>Enhanced Multi-Client System</h1>
                <p><em>Data Mapping Validation Platform</em></p>
            </div>
        """, unsafe_allow_html=True)
        
        # Login form container
        st.markdown("""
            <div style="
                background: linear-gradient(135deg, #f8f9fa, #e9ecef);
                border: 2px solid #007bff;
                border-radius: 15px;
                padding: 25px;
                margin: 20px 0;
                box-shadow: 0 8px 25px rgba(0, 123, 255, 0.15);
            ">
        """, unsafe_allow_html=True)
        
        st.markdown("### Login to Access System")
        
        # Username input with elegant styling
        username = st.text_input(
            "Username:",
            placeholder="Enter your username",
            help="Use 'Admin' for administrative access"
        )
        
        # Password input with visibility toggle
        password_container = st.container()
        
        with password_container:
            col_pass, col_toggle = st.columns([4, 1])
            
            with col_pass:
                if st.session_state.show_password:
                    password = st.text_input(
                        "Password:",
                        type="default",
                        placeholder="Enter your password"
                    )
                else:
                    password = st.text_input(
                        "Password:",
                        type="password", 
                        placeholder="Enter your password"
                    )
            
            with col_toggle:
                st.markdown("<br>", unsafe_allow_html=True)
                if st.button("👁" if not st.session_state.show_password else "🙈"):
                    st.session_state.show_password = not st.session_state.show_password
                    st.rerun()
        
        # Login button with elegant spacing
        st.markdown("<br>", unsafe_allow_html=True)
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 2, 1])
        
        with col_btn2:
            login_clicked = st.button(
                "🔐 Login",
                type="primary",
                use_container_width=True
            )
        
        # Process login attempt
        if login_clicked:
            if username and password:
                with st.spinner("Authenticating credentials..."):
                    success, user_data = authenticate_user(username, password)
                    
                    if success:
                        # Set session state for successful login
                        st.session_state.authenticated = True
                        st.session_state.username = user_data['username']
                        st.session_state.user_role = user_data['role']
                        st.session_state.client_id = user_data['client_id']
                        st.session_state.login_attempts = 0
                        
                        st.success(f"Welcome, {user_data['username']}!")
                        
                        # Set redirect flag instead of switching pages
                        if user_data['role'] == 'admin':
                            st.session_state.redirect_to = 'admin'
                        else:
                            st.session_state.redirect_to = 'client'
                        
                        st.rerun()
                    else:
                        st.session_state.login_attempts += 1
                        st.error("Invalid credentials. Please verify username and password.")
                        
                        if st.session_state.login_attempts >= 3:
                            st.warning("Multiple failed attempts detected. Please contact system administrator.")
            else:
                st.warning("Please enter both username and password.")
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        # System information with elegant presentation
        with st.expander("System Information"):
            st.markdown("""
            **Default Admin Access:**
            - Username: `Admin`
            - Password: `Maracuya123`
            
            **System Features:**
            - Multi-client data isolation
            - Advanced fuzzy matching algorithms
            - Real-time processing with progress tracking
            - Comprehensive administrative controls
            
            **Support:**
            Contact your system administrator for client account access.
            """)

def main():
    """
    The conductor of our authentication symphony,
    orchestrating the elegant flow from login to authorized access
    """
    # Configure page with aesthetic precision
    st.set_page_config(
        page_title="Enhanced Multi-Client System",
        page_icon="🔐",
        layout="centered" if not st.session_state.get('authenticated') else "wide",
        initial_sidebar_state="collapsed"
    )
    
    # Initialize session management
    initialize_session_state()
    
    # Apply custom styling for refined appearance
    st.markdown("""
        <style>
        .main { padding-top: 2rem; }
        .stTextInput > div > div > input { border-radius: 8px; }
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
    
    # Check authentication and redirect status
    if st.session_state.authenticated:
        # Display appropriate interface based on role
        if st.session_state.user_role == 'admin':
            create_admin_interface()
        else:
            create_client_interface()
    else:
        # Display login interface
        create_login_interface()

if __name__ == "__main__":
    main()