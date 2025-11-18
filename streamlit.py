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

# Your original imports
import requests
from infiviz import Infiviz
from curation import Curation

# ===== LOGGING SETUP =====
def setup_logging():
    """Setup proper logging with file and console handlers"""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Create logger
    logger = logging.getLogger('session_processor')
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # File handler with rotation (10MB max, keep 5 backups)
    file_handler = RotatingFileHandler(
        'logs/app.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
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
st.set_page_config(page_title="Session Processing Tool", page_icon="🔄", layout="wide")

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
PROCESS_SESSION_URL_TEMPLATE = "https://aicontroller.infilect.com/processed_session/{}/?infiviz_session_id={}"
SOFTTAGS = ["brand", "variant", "sku"]

# ===== SESSION STATE INITIALIZATION =====
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'user_email' not in st.session_state:
    st.session_state.user_email = None
if 'user_role' not in st.session_state:
    st.session_state.user_role = None
if 'active_tasks' not in st.session_state:
    st.session_state.active_tasks = {}

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
            <h1>🔄 Session Processing Tool</h1>
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
            else:
                success, message = request_access(req_username, req_email, req_fullname, req_password)
                
                if success:
                    st.success(message)
                    st.balloons()
                else:
                    st.error(message)

# ===== TASK CANCELLATION FUNCTIONS =====
def cancel_task(task_id):
    """Cancel a running task"""
    logger.warning(f"Task cancellation requested: {task_id} by {st.session_state.username}")
    if task_id in st.session_state.active_tasks:
        st.session_state.active_tasks[task_id]['cancelled'] = True
    update_task_status(task_id, "cancelled", error_message="Task cancelled by user")

def is_task_cancelled(task_id):
    """Check if task has been cancelled"""
    if task_id in st.session_state.active_tasks:
        return st.session_state.active_tasks[task_id].get('cancelled', False)
    return False

# ===== TASK DATABASE FUNCTIONS =====
def create_task_in_db(task_id, username, user_email, params):
    """Create task record in database"""
    try:
        logger.info(f"Creating task {task_id} for user {username}")
        logger.info(f"Task parameters: {json.dumps(params, default=str)}")
        
        # Check for concurrent tasks with same dataset_id and version_name
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

def update_task_status(task_id, status, error_message=None, result_summary=None):
    """Update task status in database"""
    try:
        logger.info(f"Updating task {task_id} status to: {status}")
        
        update_data = {"status": status}
        
        if status == "started":
            update_data["started_at"] = datetime.now().isoformat()
        elif status in ["completed", "failed", "cancelled"]:
            update_data["completed_at"] = datetime.now().isoformat()
        
        if error_message:
            update_data["error_message"] = error_message
            logger.error(f"Task {task_id} error: {error_message}")
        
        if result_summary:
            update_data["result_summary"] = result_summary
            logger.info(f"Task {task_id} summary: {json.dumps(result_summary)}")
        
        supabase.table("tasks").update(update_data).eq("task_id", task_id).execute()
    except Exception as e:
        logger.error(f"Error updating task {task_id}: {str(e)}", exc_info=True)

def get_user_tasks(username):
    """Get all tasks for a user"""
    try:
        response = supabase.table("tasks").select("*").eq("username", username).order("created_at", desc=True).execute()
        return response.data
    except Exception as e:
        logger.error(f"Error fetching tasks for {username}: {str(e)}", exc_info=True)
        return []

# ===== CORE PROCESSING FUNCTIONS =====
def fetch_output_from_ai_controller(session_id, client_id):
    """Fetch processed output from AI controller"""
    payload = {"infiviz_session_id": session_id}
    headers = {"APIKEY": APIKEY}
    url = PROCESS_SESSION_URL_TEMPLATE.format(client_id, session_id)
    
    try:
        result = requests.get(url, headers=headers, data=payload, timeout=30)
        result.raise_for_status()
        response_path = result.json()[0]["output"]
        
        try:
            response = requests.get(response_path, timeout=30).json()
            if response.get("status") == "success":
                return response
            else:
                logger.warning(f"Session {session_id} returned non-success status")
        except Exception as e:
            logger.error(f"Failed to fetch JSON from {response_path} for session {session_id}: {str(e)}")
    except Exception as e:
        logger.error(f"Error fetching output for session {session_id}: {str(e)}")
    
    return None

def fetch_and_sample_sessions(client_id, start_date, end_date, photo_types, 
                               category_types, channel_types, sample_per_channel):
    """Fetch and sample sessions from Infiviz"""
    try:
        logger.info(f"Fetching sessions for client {client_id} from {start_date} to {end_date}")
        
        inf = Infiviz()
        inf.add_variables(client_id, category_types, channel_types, photo_types, end_date, start_date)
        inf.get_combinations(processed=True)
        
        all_data = inf.all_sessions
        logger.info(f"Total sessions found: {len(all_data)}")

        # Remove duplicates
        seen = set()
        filtered_data = []
        for item in all_data:
            sess_id = item["session_id"]
            if sess_id not in seen:
                seen.add(sess_id)
                filtered_data.append(item)

        # Group by category
        category2sess_id = {}
        for item in filtered_data:
            cat_id = item["store_channel_id"]
            category2sess_id.setdefault(cat_id, []).append(item["session_id"])

        final_sess_ids = []
        for val in category2sess_id.values():
            final_sess_ids.extend(val)

        sampled_data = [item for item in filtered_data if item["session_id"] in final_sess_ids]
        logger.info(f"Total sessions sampled: {len(sampled_data)}")
        
        return sampled_data
    except Exception as e:
        logger.error(f"Error in fetch_and_sample_sessions: {str(e)}", exc_info=True)
        raise

# ===== TASK EXECUTION WITH PROPER CANCELLATION =====
def run_task(task_id, params):
    """Execute task with proper logging and cancellation support"""
    # Initialize task in active tasks
    st.session_state.active_tasks[task_id] = {'cancelled': False}
    
    # Create cancel button in sidebar
    cancel_container = st.sidebar.container()
    with cancel_container:
        st.markdown("---")
        st.warning("⚙️ **Task Running**")
        st.markdown(f"**Task ID:** `{task_id[:8]}...`")
        if st.button("🛑 Cancel Task", type="secondary", use_container_width=True, key=f"cancel_{task_id}"):
            cancel_task(task_id)
            st.error("Task cancelled!")
            logger.warning(f"Task {task_id} cancelled by user button")
            st.rerun()
    
    try:
        logger.info(f"Starting task execution: {task_id}")
        update_task_status(task_id, "started")
        
        # Step 1: Fetch sessions
        with st.status("**Step 1/3:** Fetching and sampling sessions...", expanded=True) as status:
            if is_task_cancelled(task_id):
                logger.warning(f"Task {task_id} cancelled at Step 1")
                st.warning("❌ Task cancelled")
                return False
            
            try:
                sampled_sessions = fetch_and_sample_sessions(
                    params["client_id"], params["start_date"], params["end_date"],
                    params["photo_types"], params["category_types"],
                    params["channel_types"], params["sample_per_channel"]
                )
                st.write(f"✅ Found **{len(sampled_sessions)}** sessions")
                status.update(label=f"✅ Step 1/3: {len(sampled_sessions)} sessions", state="complete")
                logger.info(f"Task {task_id}: Step 1 complete - {len(sampled_sessions)} sessions")
            except Exception as e:
                logger.error(f"Task {task_id}: Step 1 failed - {str(e)}", exc_info=True)
                raise
        
        # Step 2: Download responses
        with st.status("**Step 2/3:** Downloading processed outputs...", expanded=True) as status:
            if is_task_cancelled(task_id):
                logger.warning(f"Task {task_id} cancelled at Step 2")
                st.warning("❌ Task cancelled")
                status.update(label="❌ Step 2/3: Cancelled", state="error")
                return False
            
            progress_bar = st.progress(0)
            session_ids = [s["session_id"] for s in sampled_sessions]
            responses = []
            failed_count = 0
            
            for idx, sess_id in enumerate(session_ids):
                # Check for cancellation every 10 iterations
                if idx % 10 == 0 and is_task_cancelled(task_id):
                    logger.warning(f"Task {task_id} cancelled after {idx}/{len(session_ids)} downloads")
                    st.warning(f"❌ Task cancelled after {idx}/{len(session_ids)} downloads")
                    status.update(label="❌ Step 2/3: Cancelled", state="error")
                    return False
                
                resp = fetch_output_from_ai_controller(sess_id, params["client_id"])
                if resp:
                    responses.append(resp)
                else:
                    failed_count += 1
                
                progress_bar.progress((idx + 1) / len(session_ids))
            
            logger.info(f"Task {task_id}: Step 2 complete - {len(responses)} successful, {failed_count} failed")
            st.write(f"✅ Downloaded **{len(responses)}** responses ({failed_count} failed)")
            status.update(label=f"✅ Step 2/3: {len(responses)} responses", state="complete")
        
        # Handle zero responses
        if len(responses) == 0:
            error_msg = "No valid responses downloaded from AI controller"
            logger.error(f"Task {task_id}: {error_msg}")
            update_task_status(task_id, "failed", error_message=error_msg)
            st.error(f"❌ {error_msg}")
            return False
        
        # Step 3: Upload to curation
        with st.status("**Step 3/3:** Uploading to curation...", expanded=True) as status:
            if is_task_cancelled(task_id):
                logger.warning(f"Task {task_id} cancelled at Step 3")
                st.warning("❌ Task cancelled")
                status.update(label="❌ Step 3/3: Cancelled", state="error")
                return False
            
            progress_bar = st.progress(0)
            cur = Curation()
            cur.add_variables(params["dataset_id"], params["version_name"], CURATION_TOKEN, SOFTTAGS)
            
            success_count = 0
            failed_sessions = []
            
            for idx, resp in enumerate(responses):
                # Check for cancellation every 10 iterations
                if idx % 10 == 0 and is_task_cancelled(task_id):
                    logger.warning(f"Task {task_id} cancelled after {success_count}/{len(responses)} uploads")
                    st.warning(f"❌ Task cancelled after {success_count}/{len(responses)} uploads")
                    status.update(label="❌ Step 3/3: Cancelled", state="error")
                    return False
                
                try:
                    cur.upload2curation(resp)
                    success_count += 1
                except Exception as e:
                    session_id = resp.get('session_id', 'unknown')
                    failed_sessions.append(session_id)
                    logger.error(f"Failed to upload session {session_id}: {str(e)}")
                    st.warning(f"Failed session {session_id}: {str(e)}")
                
                progress_bar.progress((idx + 1) / len(responses))
            
            logger.info(f"Task {task_id}: Step 3 complete - {success_count} uploaded, {len(failed_sessions)} failed")
            st.write(f"✅ Uploaded **{success_count}/{len(responses)}**")
            if failed_sessions:
                st.warning(f"⚠️ {len(failed_sessions)} sessions failed to upload")
            status.update(label=f"✅ Step 3/3: Complete", state="complete")
        
        # Clear cancel state
        if task_id in st.session_state.active_tasks:
            del st.session_state.active_tasks[task_id]
        
        summary = {
            "total_sessions": len(sampled_sessions),
            "successful_responses": len(responses),
            "uploaded_to_curation": success_count,
            "failed_uploads": len(failed_sessions),
            "version_name": params["version_name"]
        }
        
        logger.info(f"Task {task_id} completed successfully: {json.dumps(summary)}")
        update_task_status(task_id, "completed", result_summary=summary)
        
        st.success("🎉 **Task completed successfully!**")
        st.json(summary)
        st.balloons()
        
        return True
        
    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        logger.error(f"Task {task_id} failed: {error_msg}")
        update_task_status(task_id, "failed", error_message=error_msg)
        st.error(f"❌ **Task failed:** {str(e)}")
        with st.expander("View full error traceback"):
            st.code(error_msg)
        return False
    finally:
        # Clear the cancel button and active task
        cancel_container.empty()
        if task_id in st.session_state.active_tasks:
            del st.session_state.active_tasks[task_id]

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
                    # Read last 1000 lines
                    lines = f.readlines()
                    recent_logs = ''.join(lines[-1000:])
                
                # Filter options
                log_filter = st.selectbox(
                    "Filter by level:",
                    ["All", "ERROR", "WARNING", "INFO"]
                )
                
                if log_filter != "All":
                    filtered_lines = [line for line in lines[-1000:] if log_filter in line]
                    recent_logs = ''.join(filtered_lines)
                
                st.code(recent_logs, language="log")
                
                # Download button
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
    
    # Navigation
    pages = ["🆕 New Task", "📊 My Tasks", "📈 Summary"]
    if st.session_state.user_role == "admin":
        pages.insert(0, "👑 Admin Panel")
    
    page = st.radio("Navigation", pages, label_visibility="collapsed")
    
    st.markdown("---")
    if st.button("🚪 Logout", use_container_width=True):
        logout()

# ===== PAGES =====
if page == "👑 Admin Panel":
    show_admin_panel()

# elif page == "🆕 New Task":
#     st.title("🆕 Create New Task")
    
#     with st.form("task_form"):
#         col1, col2 = st.columns(2)
        
#         with col1:
#             client_id = st.text_input("Client ID", value="arabian-oasis-al-seer-uae")
#             start_date = st.date_input("Start Date", value=datetime(2025, 8, 1))
#             end_date = st.date_input("End Date", value=datetime(2025, 10, 31))
#             photo_types = st.text_input("Photo Types", value="shelf")
        
#         with col2:
#             category_types = st.text_input("Category Types", value="microwave-pop-corn")
#             dataset_id = st.number_input("Dataset ID", value=384, min_value=1)
#             version_name = st.text_input("Version Name (optional)", value="")
#             sample_per_channel = st.number_input("Sample Per Channel", value=800, min_value=1)
        
#         channel_types = st.text_area("Channel Types", value="sup-mkt-wh-supply,sml-grcry-upto-20-m2", height=80)
        
#         submit = st.form_submit_button("▶️ Run Task", type="primary", use_container_width=True)
    
#     if submit:
#         if not version_name:
#             version_name = f"{category_types}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
        
#         task_id = str(uuid.uuid4())
        
#         params = {
#             "client_id": client_id,
#             "start_date": start_date.strftime("%Y-%m-%d"),
#             "end_date": end_date.strftime("%Y-%m-%d"),
#             "photo_types": photo_types,
#             "category_types": category_types,
#             "channel_types": channel_types,
#             "dataset_id": dataset_id,
#             "version_name": version_name,
#             "sample_per_channel": sample_per_channel
#         }
        
#         create_task_in_db(task_id, st.session_state.username, st.session_state.user_email, params)
        
#         st.markdown("---")
#         st.markdown(f"### ⚙️ Processing: `{task_id}`")
#         run_task(task_id, params)


# Initialize session state for metadata
if 'metadata' not in st.session_state:
    st.session_state.metadata = None
if 'last_client_id' not in st.session_state:
    st.session_state.last_client_id = ""

def fetch_metadata(client_id):
    """Fetch metadata from API based on client_id"""
    try:
        meta_info_url = f"https://public.infiviz.ai/api/v1/meta_info/?client_id={client_id}"
        response = requests.get(meta_info_url)
        response.raise_for_status()
        meta_info = response.json()["data"]
        
        # Extract options
        photo_types = [i["photo_type_name"] for i in meta_info["photo_types"]]
        category_types = [i["category_type_name"] for i in meta_info["category_types"]]
        channel_types = [i["channel_type_name"] for i in meta_info["channel_types"]]
        
        return {
            "photo_types": photo_types,
            "category_types": category_types,
            "channel_types": channel_types
        }
    except Exception as e:
        st.error(f"Error fetching metadata: {str(e)}")
        return None

# Page: New Task
if page == "🆕 New Task":
    st.title("🆕 Create New Task")
    
    # Client ID input and fetch button (OUTSIDE the form)
    col_a, col_b = st.columns([3, 1])
    with col_a:
        client_id_input = st.text_input(
            "Client ID", 
            value="arabian-oasis-al-seer-uae",
            key="client_id_input"
        )
    with col_b:
        st.write("")  # Spacer
        st.write("")  # Spacer
        fetch_button = st.button("🔄 Fetch Options", type="secondary", use_container_width=True)
    
    # Fetch metadata when button is clicked
    if fetch_button:
        with st.spinner("Fetching metadata..."):
            st.session_state.metadata = fetch_metadata(client_id_input)
            st.session_state.last_client_id = client_id_input
            if st.session_state.metadata:
                st.success("✓ Options loaded successfully!")
    
    # Check if metadata is available
    if st.session_state.metadata:
        photo_options = st.session_state.metadata["photo_types"]
        category_options = st.session_state.metadata["category_types"]
        channel_options = st.session_state.metadata["channel_types"]
    else:
        photo_options = []
        category_options = []
        channel_options = []
        st.warning("⚠️ Please fetch options for the client ID first")
    
    # Main form with dynamic options
    with st.form("task_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input("Start Date", value=datetime(2025, 8, 1))
            end_date = st.date_input("End Date", value=datetime(2025, 10, 31))
            
            # Multi-select for photo types
            photo_types_selected = st.multiselect(
                "Photo Types",
                options=photo_options,
                default=None,
                help="Select one or more photo types, or leave empty for all"
            )
        
        with col2:
            dataset_id = st.number_input("Dataset ID", value=384, min_value=1)
            version_name = st.text_input("Version Name (optional)", value="")
            sample_per_channel = st.number_input("Sample Per Channel", value=800, min_value=1)
        
        # Multi-select for category types
        category_types_selected = st.multiselect(
            "Category Types",
            options=category_options,
            default=None,
            help="Select one or more categories, or leave empty for all"
        )
        
        # Multi-select for channel types
        channel_types_selected = st.multiselect(
            "Channel Types",
            options=channel_options,
            default=None,
            help="Select one or more channels, or leave empty for all"
        )
        
        submit = st.form_submit_button("▶️ Run Task", type="primary", use_container_width=True)
    
    if submit:
        if not st.session_state.metadata:
            st.error("❌ Please fetch metadata first before submitting")
        else:
            # Process selections - if empty, use all options
            photo_types_final = ",".join(photo_types_selected) if photo_types_selected else ",".join(photo_options)
            category_types_final = ",".join(category_types_selected) if category_types_selected else ",".join(category_options)
            channel_types_final = ",".join(channel_types_selected) if channel_types_selected else ",".join(channel_options)
            
            if not version_name:
                version_name = f"{category_types_final.split(',')[0]}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
            
            task_id = str(uuid.uuid4())
            
            params = {
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
            
            st.markdown("---")
            st.markdown(f"### ⚙️ Processing: `{task_id}`")
            run_task(task_id, params)

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
                
                if task["status"] == "completed" and task.get("result_summary"):
                    st.success("✅ Task Summary:")
                    st.json(task["result_summary"])
                
                if task["status"] == "cancelled":
                    st.warning("🛑 This task was cancelled by the user.")
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
            
            # Status distribution chart
            status_counts = pd.DataFrame(tasks)['status'].value_counts()
            st.subheader("Task Status Distribution")
            st.bar_chart(status_counts)
            
            # Recent tasks table
            st.markdown("---")
            st.subheader("Recent Tasks")
            df = pd.DataFrame(tasks)
            display_cols = ['version_name', 'status', 'client_id', 'dataset_id', 'created_at', 'completed_at']
            df_display = df[display_cols].head(10)
            st.dataframe(df_display, use_container_width=True)
    else:
        st.info("No tasks to summarize yet!")