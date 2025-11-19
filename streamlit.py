import streamlit as st
from datetime import datetime
from supabase import create_client
import hashlib
import traceback
import json
import uuid
import logging
from logging.handlers import RotatingFileHandler
import os
import subprocess  # ✅ ADDED
import sys         # ✅ ADDED
# Your original imports
import requests
from infiviz import Infiviz
from curation import Curation


# ===== LOGGING SETUP =====
def setup_logging():
    """Setup proper logging with file and console handlers"""
    os.makedirs('logs', exist_ok=True)
    
    logger = logging.getLogger('session_processor')
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    file_handler = RotatingFileHandler(
        'logs/app.log',
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


logger = setup_logging()


# ===== PAGE CONFIG =====
st.set_page_config(page_title="Automated Data Upload Tool (Built solely for DA Team)", page_icon="🔄", layout="wide")


# ===== SUPABASE CONNECTION =====
@st.cache_resource
def init_supabase():
    supabase_url = st.secrets["connections"]["supabase"]["SUPABASE_URL"]
    supabase_key = st.secrets["connections"]["supabase"]["SUPABASE_KEY"]
    logger.info("Initializing Supabase connection")
    return create_client(supabase_url, supabase_key)


supabase = init_supabase()


# ===== SECRETS =====
APIKEY = st.secrets["APIKEY"]
CURATION_TOKEN = st.secrets["CURATION_TOKEN"]


# ===== SESSION STATE INITIALIZATION =====
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'user_email' not in st.session_state:
    st.session_state.user_email = None
if 'user_role' not in st.session_state:
    st.session_state.user_role = None
# ❌ REMOVED: if 'active_tasks' not in st.session_state


# ===== PASSWORD FUNCTIONS =====
def hash_password(password):
    """Hash a password using SHA-256"""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def verify_password(password, password_hash):
    """Verify a password against its hash"""
    return hash_password(password) == password_hash


def authenticate_user(username, password):
    """Authenticate user credentials"""
    try:
        logger.info(f"Authentication attempt for user: {username}")
        response = supabase.table("users").select("*").eq("username", username).execute()
        
        if not response.data:
            logger.warning(f"Authentication failed: Username not found - {username}")
            return False, "Username not found"
        
        user = response.data[0]
        
        if user['status'] != 'approved':
            logger.warning(f"Authentication failed: Account status is {user['status']} for {username}")
            return False, f"Account is {user['status']}. Please contact admin for approval."
        
        if verify_password(password, user['password_hash']):
            supabase.table("users").update({
                "last_login": datetime.now().isoformat()
            }).eq("username", username).execute()
            logger.info(f"User authenticated successfully: {username}")
            return True, user
        else:
            logger.warning(f"Authentication failed: Incorrect password for {username}")
            return False, "Incorrect password"
    except Exception as e:
        logger.error(f"Authentication error for {username}: {str(e)}", exc_info=True)
        return False, f"Error: {str(e)}"


def request_access(username, email, full_name, password):
    """Submit access request"""
    try:
        logger.info(f"Access request from: {username} ({email})")
        
        response = supabase.table("users").select("username, email").or_(
            f"username.eq.{username},email.eq.{email}"
        ).execute()
        
        if response.data:
            existing = response.data[0]
            if existing['username'] == username:
                logger.warning(f"Access request failed: Username exists - {username}")
                return False, "Username already exists"
            if existing['email'] == email:
                logger.warning(f"Access request failed: Email exists - {email}")
                return False, "Email already registered"
        
        supabase.table("users").insert({
            "username": username,
            "password_hash": hash_password(password),
            "email": email,
            "full_name": full_name,
            "status": "pending"
        }).execute()
        
        logger.info(f"Access request created for: {username}")
        return True, "Access request submitted! Admin will review your request."
    except Exception as e:
        logger.error(f"Error creating access request for {username}: {str(e)}", exc_info=True)
        return False, f"Error: {str(e)}"


def logout():
    """Logout user"""
    logger.info(f"User logged out: {st.session_state.username}")
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.user_email = None
    st.session_state.user_role = None
    st.rerun()


# ===== LOGIN PAGE =====
def show_login_page():
    st.markdown("""
        <div style='text-align: center; padding: 2rem 0;'>
            <h1>🔄 Data upload Tool (Solely built for DA Team)</h1>
            <p style='color: #666;'>Please login or request access</p>
        </div>
    """, unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["🔐 Login", "📝 Request Access"])
    
    with tab1:
        st.markdown("### Login to Your Account")
        
        with st.form("login_form"):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input("Password", type="password", placeholder="Enter your password")
            submit = st.form_submit_button("🔓 Login", type="primary", use_container_width=True)
        
        if submit:
            if not username or not password:
                st.error("Please enter both username and password")
            else:
                success, result = authenticate_user(username, password)
                
                if success:
                    st.session_state.authenticated = True
                    st.session_state.username = result['username']
                    st.session_state.user_email = result['email']
                    st.session_state.user_role = result['role']
                    st.success(f"Welcome back, {result['full_name']}!")
                    st.rerun()
                else:
                    st.error(f"❌ {result}")
    
    with tab2:
        st.markdown("### Request Access")
        st.info("Fill in your details below. An admin will review and approve your request.")
        
        with st.form("request_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                req_username = st.text_input("Username", placeholder="Choose a username")
                req_email = st.text_input("Email", placeholder="your.email@company.com")
            
            with col2:
                req_fullname = st.text_input("Full Name", placeholder="Your full name")
                req_password = st.text_input("Password", type="password", placeholder="Choose a password")
            
            submit_request = st.form_submit_button("📨 Submit Request", type="secondary", use_container_width=True)
        
        if submit_request:
            if not all([req_username, req_email, req_fullname, req_password]):
                st.error("Please fill in all fields")
            elif len(req_password) < 6:
                st.error("Password must be at least 6 characters")
            elif "@" not in req_email:
                st.error("Please enter a valid email")
            elif "infilect.com" not in req_email:
                st.error("You are using an Mail ID outside of the Infilect Domain ---> Gotcha Muhehe")
            else:
                success, message = request_access(req_username, req_email, req_fullname, req_password)
                
                if success:
                    st.success(message)
                    st.balloons()
                else:
                    st.error(message)


# ===== TASK DATABASE FUNCTIONS =====
def create_task_in_db(task_id, username, user_email, params):
    """Create task record in database"""
    try:
        logger.info(f"Creating task {task_id} for user {username}")
        logger.info(f"Task parameters: {json.dumps(params, default=str)}")
        
        # Check for concurrent tasks
        response = supabase.table("tasks").select("*").eq(
            "dataset_id", params["dataset_id"]
        ).eq("version_name", params["version_name"]).in_(
            "status", ["queued", "started"]
        ).execute()
        
        if response.data:
            existing_task = response.data[0]
            logger.warning(f"Concurrent task detected: {existing_task['task_id']} by {existing_task['username']}")
            st.warning(f"⚠️ Warning: Another task is already running with the same dataset ({params['dataset_id']}) and version ({params['version_name']})")
            st.warning(f"Started by: {existing_task['username']} at {existing_task['created_at'][:19]}")
            st.info("This may cause data conflicts. Consider using a different version name.")
        
        supabase.table("tasks").insert({
            "task_id": task_id,
            "username": username,
            "user_email": user_email,
            "client_id": params["client_id"],
            "start_date": params["start_date"],
            "end_date": params["end_date"],
            "photo_types": params["photo_types"],
            "category_types": params["category_types"],
            "channel_types": params["channel_types"],
            "dataset_id": params["dataset_id"],
            "version_name": params["version_name"],
            "status": "queued"
        }).execute()
        
        logger.info(f"Task created successfully: {task_id}")
    except Exception as e:
        logger.error(f"Error creating task {task_id}: {str(e)}", exc_info=True)
        raise


def get_user_tasks(username):
    """Get all tasks for a user"""
    try:
        response = supabase.table("tasks").select("*").eq("username", username).order("created_at", desc=True).execute()
        return response.data
    except Exception as e:
        logger.error(f"Error fetching tasks for {username}: {str(e)}", exc_info=True)
        return []


# ✅ ADDED: New cancellation function for background tasks
def cancel_task_in_db(task_id):
    """Mark task as cancelled in database"""
    try:
        logger.warning(f"Task cancellation requested: {task_id} by {st.session_state.username}")
        supabase.table("tasks").update({
            "status": "cancelled",
            "completed_at": datetime.now().isoformat(),
            "error_message": "Task cancelled by user"
        }).eq("task_id", task_id).execute()
        logger.info(f"Task {task_id} marked as cancelled in database")
    except Exception as e:
        logger.error(f"Error cancelling task {task_id}: {str(e)}", exc_info=True)


# ❌ REMOVED: Old cancel_task() and is_task_cancelled() functions
# ❌ REMOVED: Old run_task() function with st.status blocks


# ✅ ADDED: New background task launcher
def launch_background_task(task_id, params):
    """Launch task_runner.py as a completely detached background process"""
    try:
        # Prepare parameters as JSON file
        params_file = f"task_params_{task_id}.json"
        with open(params_file, 'w') as f:
            json.dump(params, f)
        
        # Get Python executable
        python_executable = sys.executable
        
        # Launch detached process
        if os.name == 'nt':  # Windows
            process = subprocess.Popen(
                [python_executable, "task_runner.py", task_id, params_file],
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
            )
        else:  # Unix/Linux/Mac
            process = subprocess.Popen(
                [python_executable, "task_runner.py", task_id, params_file],
                start_new_session=True
            )
        
        logger.info(f"Background task launched: {task_id} (PID: {process.pid})")
        return True, f"Task started in background (PID: {process.pid})"
    except Exception as e:
        logger.error(f"Error launching background task {task_id}: {str(e)}", exc_info=True)
        return False, f"Error: {str(e)}"


# ===== ADMIN PANEL =====
def show_admin_panel():
    st.title("👑 Admin Panel - User Management")
    
    tab1, tab2, tab3 = st.tabs(["📋 Pending Requests", "👥 All Users", "📜 View Logs"])
    
    with tab1:
        st.markdown("### Pending Access Requests")
        st.info("👇 Click **Approve** or **Reject** for each pending user below")
        
        pending = supabase.table("users").select("*").eq("status", "pending").order("requested_at", desc=True).execute()
        
        if not pending.data:
            st.success("✅ No pending requests - all users have been reviewed!")
        else:
            st.warning(f"⏳ **{len(pending.data)} pending request(s)** waiting for your approval")
            
            for user in pending.data:
                with st.expander(f"👤 {user['full_name']} (@{user['username']})", expanded=True):
                    col1, col2, col3 = st.columns([2, 2, 1])
                    
                    with col1:
                        st.markdown(f"**Email:** {user['email']}")
                        st.markdown(f"**Username:** {user['username']}")
                    
                    with col2:
                        st.markdown(f"**Full Name:** {user['full_name']}")
                        st.markdown(f"**Requested:** {user['requested_at'][:19]}")
                    
                    with col3:
                        if st.button(f"✅ Approve", key=f"approve_{user['id']}", type="primary", use_container_width=True):
                            supabase.table("users").update({
                                "status": "approved",
                                "approved_at": datetime.now().isoformat(),
                                "approved_by": st.session_state.username
                            }).eq("id", user['id']).execute()
                            logger.info(f"User {user['username']} approved by {st.session_state.username}")
                            st.success(f"✅ Approved {user['username']}!")
                            st.rerun()
                        
                        if st.button(f"❌ Reject", key=f"reject_{user['id']}", use_container_width=True):
                            supabase.table("users").update({
                                "status": "rejected"
                            }).eq("id", user['id']).execute()
                            logger.info(f"User {user['username']} rejected by {st.session_state.username}")
                            st.warning(f"Rejected {user['username']}")
                            st.rerun()
    
    with tab2:
        st.markdown("### All Users")
        
        users = supabase.table("users").select("*").order("created_at", desc=True).execute()
        
        if users.data:
            import pandas as pd
            df = pd.DataFrame(users.data)
            
            display_columns = ['username', 'full_name', 'email', 'role', 'status', 'last_login', 'created_at']
            df_display = df[display_columns]
            
            st.dataframe(df_display, use_container_width=True)
            
            st.markdown(f"""
            **Legend:**
            - 🟢 Approved: {len(df[df['status'] == 'approved'])} users
            - 🟡 Pending: {len(df[df['status'] == 'pending'])} users
            - 🔴 Rejected: {len(df[df['status'] == 'rejected'])} users
            """)
    
    with tab3:
        st.markdown("### 📜 Application Logs")
        st.info("View recent application logs (last 1000 lines)")
        
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🔄 Refresh Logs"):
                st.rerun()
        
        try:
            if os.path.exists('logs/app.log'):
                with open('logs/app.log', 'r') as f:
                    lines = f.readlines()
                    recent_logs = ''.join(lines[-1000:])
                
                log_filter = st.selectbox(
                    "Filter by level:",
                    ["All", "ERROR", "WARNING", "INFO"]
                )
                
                if log_filter != "All":
                    filtered_lines = [line for line in lines[-1000:] if log_filter in line]
                    recent_logs = ''.join(filtered_lines)
                
                st.code(recent_logs, language="log")
                
                st.download_button(
                    label="⬇️ Download Full Log",
                    data=open('logs/app.log', 'r').read(),
                    file_name=f"app_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                    mime="text/plain"
                )
            else:
                st.warning("No log file found yet. Logs will appear after first task execution.")
        except Exception as e:
            st.error(f"Error reading logs: {str(e)}")


# ===== CHECK AUTHENTICATION =====
if not st.session_state.authenticated:
    show_login_page()
    st.stop()


# ===== SIDEBAR =====
with st.sidebar:
    st.markdown(f"### 👋 {st.session_state.username}")
    st.markdown(f"📧 {st.session_state.user_email}")
    st.markdown(f"🏷️ Role: **{st.session_state.user_role}**")
    st.markdown("---")
    
    pages = ["🆕 New Task", "📊 My Tasks", "📈 Summary"]
    if st.session_state.user_role == "admin":
        pages.insert(0, "👑 Admin Panel")
    
    page = st.radio("Navigation", pages, label_visibility="collapsed")
    
    st.markdown("---")
    if st.button("🚪 Logout", use_container_width=True):
        logout()


# ===== METADATA FETCHING =====
if 'metadata' not in st.session_state:
    st.session_state.metadata = None
if 'last_client_id' not in st.session_state:
    st.session_state.last_client_id = ""


@st.cache_data(ttl=3600)
def fetch_metadata(client_id):
    """Fetch metadata from API based on client_id (cached)"""
    try:
        meta_info_url = f"https://public.infiviz.ai/api/v1/meta_info/?client_id={client_id}"
        response = requests.get(meta_info_url, timeout=10)
        response.raise_for_status()
        meta_info = response.json()["data"]
        
        photo_types = [i["photo_type_name"] for i in meta_info["photo_types"]]
        category_types = [i["category_type_name"] for i in meta_info["category_types"]]
        channel_types = [i["channel_type_name"] for i in meta_info["channel_types"]]
        
        return {
            "photo_types": photo_types,
            "category_types": category_types,
            "channel_types": channel_types,
            "success": True
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ===== PAGES =====
if page == "👑 Admin Panel":
    show_admin_panel()


elif page == "🆕 New Task":
    st.title("🆕 Create New Task")
    
    client_id_input = st.text_input(
        "Client ID", 
        value="arabian-oasis-al-seer-uae",
        key="client_id_input",
        help="Enter client ID to automatically fetch available options"
    )
    
    if client_id_input and client_id_input != st.session_state.last_client_id:
        with st.spinner("🔄 Fetching options..."):
            result = fetch_metadata(client_id_input)
            if result.get("success"):
                st.session_state.metadata = result
                st.session_state.last_client_id = client_id_input
                st.success("✓ Options loaded!")
            else:
                st.error(f"❌ Error: {result.get('error', 'Unknown error')}")
                st.session_state.metadata = None
    
    if st.session_state.metadata and st.session_state.metadata.get("success"):
        photo_options = st.session_state.metadata["photo_types"]
        category_options = st.session_state.metadata["category_types"]
        channel_options = st.session_state.metadata["channel_types"]
        options_available = True
    else:
        photo_options = []
        category_options = []
        channel_options = []
        options_available = False
    
    st.markdown("---")
    
    with st.form("task_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input("Start Date", value=datetime(2025, 8, 1))
            end_date = st.date_input("End Date", value=datetime(2025, 10, 31))
            
            photo_types_selected = st.multiselect(
                "📸 Photo Types",
                options=photo_options,
                help="Select photo types (empty = all)",
                disabled=not options_available
            )
        
        with col2:
            dataset_id = st.number_input("Dataset ID", value=384, min_value=1)
            version_name = st.text_input("Version Name (optional)", value="")
            sample_per_channel = st.number_input("Sample Per Channel", value=800, min_value=1)
        
        category_types_selected = st.multiselect(
            "🏷️ Category Types",
            options=category_options,
            help="Select categories (empty = all)",
            disabled=not options_available
        )
        
        channel_types_selected = st.multiselect(
            "📊 Channel Types",
            options=channel_options,
            help="Select channels (empty = all)",
            disabled=not options_available
        )
        
        submit = st.form_submit_button(
            "▶️ Run Task", 
            type="primary", 
            use_container_width=True,
            disabled=not options_available
        )
    
    if submit:
        photo_types_final = ",".join(photo_types_selected) if photo_types_selected else ",".join(photo_options)
        category_types_final = ",".join(category_types_selected) if category_types_selected else ",".join(category_options)
        channel_types_final = ",".join(channel_types_selected) if channel_types_selected else ",".join(channel_options)
        
        if not version_name:
            category_name = category_types_selected[0] if category_types_selected else category_options[0] if category_options else "task"
            version_name = f"{category_name}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
        
        task_id = str(uuid.uuid4())
        
        # ✅ MODIFIED: Added task_id, username, user_email to params for background task
        params = {
            "task_id": task_id,
            "username": st.session_state.username,
            "user_email": st.session_state.user_email,
            "client_id": client_id_input,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "photo_types": photo_types_final,
            "category_types": category_types_final,
            "channel_types": channel_types_final,
            "dataset_id": dataset_id,
            "version_name": version_name,
            "sample_per_channel": sample_per_channel
        }
        
        create_task_in_db(task_id, st.session_state.username, st.session_state.user_email, params)
        
        # ✅ CHANGED: Use background task launcher instead of run_task()
        success, message = launch_background_task(task_id, params)
        
        if success:
            st.success(f"✅ {message}")
            st.info(f"**Task ID:** `{task_id}`")
            st.info("ℹ️ The task is now running in the background. You can close this tab and check status later in 'My Tasks'.")
            st.balloons()
        else:
            st.error(f"❌ Failed to launch task: {message}")


elif page == "📊 My Tasks":
    st.title("📊 My Tasks")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if st.button("🔄 Refresh"):
            st.rerun()
    
    tasks = get_user_tasks(st.session_state.username)
    
    if not tasks:
        st.info("No tasks yet!")
    else:
        for task in tasks:
            status_emoji = {
                "queued": "⏳", 
                "started": "🔄", 
                "completed": "✅", 
                "failed": "❌",
                "cancelled": "🛑"
            }.get(task["status"], "❓")
            
            with st.expander(f"{status_emoji} {task['version_name']} - {task['status'].upper()} ({task['created_at'][:19]})"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Task ID:** `{task['task_id']}`")
                    st.markdown(f"**Client:** {task['client_id']}")
                    st.markdown(f"**Category:** {task['category_types']}")
                    st.markdown(f"**Dataset ID:** {task['dataset_id']}")
                
                with col2:
                    st.markdown(f"**Status:** {task['status']}")
                    if task.get("started_at"):
                        st.markdown(f"**Started:** {task['started_at'][:19]}")
                    if task.get("completed_at"):
                        st.markdown(f"**Completed:** {task['completed_at'][:19]}")
                
                # ✅ ADDED: Cancel button for running tasks
                if task["status"] in ["queued", "started"]:
                    if st.button(f"🛑 Cancel Task", key=f"cancel_{task['task_id']}", type="secondary"):
                        cancel_task_in_db(task['task_id'])
                        st.warning("Task cancellation requested. The background process will stop at next checkpoint.")
                        st.rerun()
                
                if task["status"] == "completed" and task.get("result_summary"):
                    st.success("✅ Task Summary:")
                    st.json(task["result_summary"])
                
                if task["status"] == "cancelled":
                    st.warning("🛑 This task was cancelled.")
                    if task.get("error_message"):
                        st.info(task["error_message"])
                
                if task["status"] == "failed" and task.get("error_message"):
                    st.error("❌ Error Details:")
                    with st.expander("View full error"):
                        st.code(task["error_message"])
elif page == "📈 Summary":
    st.title("📈 Summary")
    
    tasks = get_user_tasks(st.session_state.username)
    
    if tasks:
        completed = sum(1 for t in tasks if t["status"] == "completed")
        failed = sum(1 for t in tasks if t["status"] == "failed")
        cancelled = sum(1 for t in tasks if t["status"] == "cancelled")
        in_progress = sum(1 for t in tasks if t["status"] in ["queued", "started"])
        
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total", len(tasks))
        col2.metric("✅ Completed", completed)
        col3.metric("🔄 In Progress", in_progress)
        col4.metric("❌ Failed", failed)
        col5.metric("🛑 Cancelled", cancelled)
        
        if len(tasks) > 0:
            st.markdown("---")
            import pandas as pd
            
            status_counts = pd.DataFrame(tasks)['status'].value_counts()
            st.subheader("Task Status Distribution")
            st.bar_chart(status_counts)
            
            st.markdown("---")
            st.subheader("Recent Tasks")
            df = pd.DataFrame(tasks)
            display_cols = ['version_name', 'status', 'client_id', 'dataset_id', 'created_at', 'completed_at']
            df_display = df[display_cols].head(10)
            st.dataframe(df_display, use_container_width=True)
    else:
        st.info("No tasks to summarize yet!")
