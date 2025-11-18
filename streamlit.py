import streamlit as st
from datetime import datetime
from supabase import create_client
import hashlib
import traceback
import json
import uuid

# Your original imports
import requests
from infiviz import Infiviz
from curation import Curation

# ===== PAGE CONFIG =====
st.set_page_config(page_title="Session Processing Tool", page_icon="🔄", layout="wide")

# ===== SUPABASE CONNECTION =====
@st.cache_resource
def init_supabase():
    supabase_url = st.secrets["connections"]["supabase"]["SUPABASE_URL"]
    supabase_key = st.secrets["connections"]["supabase"]["SUPABASE_KEY"]
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

# ===== PASSWORD FUNCTIONS (USING HASHLIB) =====
def hash_password(password):
    """Hash a password using SHA-256"""
    return hashlib.sha256(password.encode('utf-8')).hexdigest()

def verify_password(password, password_hash):
    """Verify a password against its hash"""
    return hash_password(password) == password_hash

def authenticate_user(username, password):
    """Authenticate user credentials"""
    try:
        response = supabase.table("users").select("*").eq("username", username).execute()
        
        if not response.data:
            return False, "Username not found"
        
        user = response.data[0]
        
        if user['status'] != 'approved':
            return False, f"Account is {user['status']}. Please contact admin for approval."
        
        if verify_password(password, user['password_hash']):
            # Update last login
            supabase.table("users").update({
                "last_login": datetime.now().isoformat()
            }).eq("username", username).execute()
            
            return True, user
        else:
            return False, "Incorrect password"
    except Exception as e:
        return False, f"Error: {str(e)}"

def request_access(username, email, full_name, password):
    """Submit access request"""
    try:
        # Check if username or email already exists
        response = supabase.table("users").select("username, email").or_(
            f"username.eq.{username},email.eq.{email}"
        ).execute()
        
        if response.data:
            existing = response.data[0]
            if existing['username'] == username:
                return False, "Username already exists"
            if existing['email'] == email:
                return False, "Email already registered"
        
        # Create new user with pending status
        supabase.table("users").insert({
            "username": username,
            "password_hash": hash_password(password),
            "email": email,
            "full_name": full_name,
            "status": "pending"
        }).execute()
        
        return True, "Access request submitted! Admin will review your request."
    except Exception as e:
        return False, f"Error: {str(e)}"

def logout():
    """Logout user"""
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
    
    # LOGIN TAB
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
    
    # REQUEST ACCESS TAB
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

# ===== CHECK AUTHENTICATION =====
if not st.session_state.authenticated:
    show_login_page()
    st.stop()

# ===== YOUR ORIGINAL FUNCTIONS (NO CHANGES) =====
def fetch_output_from_ai_controller(session_id, client_id):
    payload = {"infiviz_session_id": session_id}
    headers = {"APIKEY": APIKEY}
    url = PROCESS_SESSION_URL_TEMPLATE.format(client_id, session_id)
    
    try:
        result = requests.get(url, headers=headers, data=payload)
        response_path = result.json()[0]["output"]
        try:
            response = requests.get(response_path).json()
        except Exception:
            print(f"Failed to fetch JSON from {response_path} for session {session_id}")
        if response.get("status") == "success":
            return response
    except Exception:
        traceback.print_exc()
    return None

def fetch_and_sample_sessions(client_id, start_date, end_date, photo_types, 
                               category_types, channel_types, sample_per_channel):
    inf = Infiviz()
    inf.add_variables(client_id, category_types, channel_types, photo_types, end_date, start_date)
    print("Fetching combinations from Infiviz...")
    inf.get_combinations(processed=True)
    
    all_data = inf.all_sessions
    print(f"Total sessions found: {len(all_data)}")

    seen = set()
    filtered_data = []
    for item in all_data:
        sess_id = item["session_id"]
        if sess_id not in seen:
            seen.add(sess_id)
            filtered_data.append(item)

    category2sess_id = {}
    for item in filtered_data:
        cat_id = item["store_channel_id"]
        category2sess_id.setdefault(cat_id, []).append(item["session_id"])

    final_sess_ids = []
    for val in category2sess_id.values():
        final_sess_ids.extend(val)

    sampled_data = [item for item in filtered_data if item["session_id"] in final_sess_ids]
    print(f"Total sessions sampled: {len(sampled_data)}")
    
    return sampled_data

def download_responses(sampled_sessions, client_id):
    session_ids = [s["session_id"] for s in sampled_sessions]
    print(f"Downloading processed outputs for {len(session_ids)} sessions...")
    responses = []

    for sess_id in session_ids:
        resp = fetch_output_from_ai_controller(sess_id, client_id)
        if resp:
            responses.append(resp)
    
    print(f"Total successful responses: {len(responses)}")
    return responses

def upload_to_curation(responses, dataset_id, version_name):
    cur = Curation()
    cur.add_variables(dataset_id, version_name, CURATION_TOKEN, SOFTTAGS)

    print(f"Uploading {len(responses)} responses to Curation...")
    success_count = 0
    for resp in responses:
        try:
            cur.upload2curation(resp)
            success_count += 1
        except Exception:
            print(f"Failed to upload session {resp.get('session_id')}")
            traceback.print_exc()
    
    return success_count

# ===== TASK DATABASE FUNCTIONS =====
def create_task_in_db(task_id, username, user_email, params):
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

def update_task_status(task_id, status, error_message=None, result_summary=None):
    update_data = {"status": status}
    
    if status == "started":
        update_data["started_at"] = datetime.now().isoformat()
    elif status in ["completed", "failed"]:
        update_data["completed_at"] = datetime.now().isoformat()
    
    if error_message:
        update_data["error_message"] = error_message
    if result_summary:
        update_data["result_summary"] = result_summary
    
    supabase.table("tasks").update(update_data).eq("task_id", task_id).execute()

def get_user_tasks(username):
    response = supabase.table("tasks").select("*").eq("username", username).order("created_at", desc=True).execute()
    return response.data

# ===== TASK EXECUTION =====
def run_task(task_id, params):
    try:
        update_task_status(task_id, "started")
        
        with st.status("**Step 1/3:** Fetching and sampling sessions...", expanded=True) as status:
            sampled_sessions = fetch_and_sample_sessions(
                params["client_id"], params["start_date"], params["end_date"],
                params["photo_types"], params["category_types"],
                params["channel_types"], params["sample_per_channel"]
            )
            st.write(f"✅ Found **{len(sampled_sessions)}** sessions")
            status.update(label=f"✅ Step 1/3: {len(sampled_sessions)} sessions", state="complete")
        
        with st.status("**Step 2/3:** Downloading processed outputs...", expanded=True) as status:
            progress_bar = st.progress(0)
            session_ids = [s["session_id"] for s in sampled_sessions]
            responses = []
            
            for idx, sess_id in enumerate(session_ids):
                resp = fetch_output_from_ai_controller(sess_id, params["client_id"])
                if resp:
                    responses.append(resp)
                progress_bar.progress((idx + 1) / len(session_ids))
            
            st.write(f"✅ Downloaded **{len(responses)}** responses")
            status.update(label=f"✅ Step 2/3: {len(responses)} responses", state="complete")
        
        with st.status("**Step 3/3:** Uploading to curation...", expanded=True) as status:
            progress_bar = st.progress(0)
            cur = Curation()
            cur.add_variables(params["dataset_id"], params["version_name"], CURATION_TOKEN, SOFTTAGS)
            
            success_count = 0
            for idx, resp in enumerate(responses):
                try:
                    cur.upload2curation(resp)
                    success_count += 1
                except Exception:
                    st.warning(f"Failed session {resp.get('session_id')}")
                progress_bar.progress((idx + 1) / len(responses))
            
            st.write(f"✅ Uploaded **{success_count}/{len(responses)}**")
            status.update(label=f"✅ Step 3/3: Complete", state="complete")
        
        summary = {
            "total_sessions": len(sampled_sessions),
            "successful_responses": len(responses),
            "uploaded_to_curation": success_count,
            "version_name": params["version_name"]
        }
        update_task_status(task_id, "completed", result_summary=summary)
        
        st.success("🎉 **Task completed successfully!**")
        st.json(summary)
        st.balloons()
        
        return True
        
    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        update_task_status(task_id, "failed", error_message=error_msg)
        st.error(f"❌ **Task failed:** {str(e)}")
        with st.expander("View error"):
            st.code(error_msg)
        return False

# ===== ADMIN PANEL (THIS IS WHERE YOU APPROVE REQUESTS!) =====
def show_admin_panel():
    st.title("👑 Admin Panel - User Management")
    
    tab1, tab2 = st.tabs(["📋 Pending Requests", "👥 All Users"])
    
    # ===== THIS IS WHERE YOU APPROVE USERS! =====
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
                            st.success(f"✅ Approved {user['username']}!")
                            st.rerun()
                        
                        if st.button(f"❌ Reject", key=f"reject_{user['id']}", use_container_width=True):
                            supabase.table("users").update({
                                "status": "rejected"
                            }).eq("id", user['id']).execute()
                            st.warning(f"Rejected {user['username']}")
                            st.rerun()
    
    # ALL USERS TAB
    with tab2:
        st.markdown("### All Users")
        
        users = supabase.table("users").select("*").order("created_at", desc=True).execute()
        
        if users.data:
            import pandas as pd
            df = pd.DataFrame(users.data)
            
            # Select and reorder columns
            display_columns = ['username', 'full_name', 'email', 'role', 'status', 'last_login', 'created_at']
            df_display = df[display_columns]
            
            # Add color coding
            def color_status(val):
                if val == 'approved':
                    return 'background-color: #d4edda'
                elif val == 'pending':
                    return 'background-color: #fff3cd'
                elif val == 'rejected':
                    return 'background-color: #f8d7da'
                return ''
            
            st.dataframe(df_display, use_container_width=True)
            
            st.markdown(f"""
            **Legend:**
            - 🟢 Approved: {len(df[df['status'] == 'approved'])} users
            - 🟡 Pending: {len(df[df['status'] == 'pending'])} users
            - 🔴 Rejected: {len(df[df['status'] == 'rejected'])} users
            """)

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

elif page == "🆕 New Task":
    st.title("🆕 Create New Task")
    
    with st.form("task_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            client_id = st.text_input("Client ID", value="arabian-oasis-al-seer-uae")
            start_date = st.date_input("Start Date", value=datetime(2025, 8, 1))
            end_date = st.date_input("End Date", value=datetime(2025, 10, 31))
            photo_types = st.text_input("Photo Types", value="shelf")
        
        with col2:
            category_types = st.text_input("Category Types", value="microwave-pop-corn")
            dataset_id = st.number_input("Dataset ID", value=384, min_value=1)
            version_name = st.text_input("Version Name (optional)", value="")
            sample_per_channel = st.number_input("Sample Per Channel", value=800, min_value=1)
        
        channel_types = st.text_area("Channel Types", value="sup-mkt-wh-supply,sml-grcry-upto-20-m2", height=80)
        
        submit = st.form_submit_button("▶️ Run Task", type="primary", use_container_width=True)
    
    if submit:
        if not version_name:
            version_name = f"{category_types}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
        
        task_id = str(uuid.uuid4())
        
        params = {
            "client_id": client_id,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "photo_types": photo_types,
            "category_types": category_types,
            "channel_types": channel_types,
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
    
    if st.button("🔄 Refresh"):
        st.rerun()
    
    tasks = get_user_tasks(st.session_state.username)
    
    if not tasks:
        st.info("No tasks yet!")
    else:
        for task in tasks:
            status_emoji = {"queued": "⏳", "started": "🔄", "completed": "✅", "failed": "❌"}[task["status"]]
            
            with st.expander(f"{status_emoji} {task['version_name']} - {task['status'].upper()} ({task['created_at'][:19]})"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Task ID:** `{task['task_id']}`")
                    st.markdown(f"**Client:** {task['client_id']}")
                    st.markdown(f"**Category:** {task['category_types']}")
                
                with col2:
                    st.markdown(f"**Status:** {task['status']}")
                    if task.get("started_at"):
                        st.markdown(f"**Started:** {task['started_at'][:19]}")
                    if task.get("completed_at"):
                        st.markdown(f"**Completed:** {task['completed_at'][:19]}")
                
                if task["status"] == "completed" and task.get("result_summary"):
                    st.json(task["result_summary"])
                
                if task["status"] == "failed" and task.get("error_message"):
                    st.error("Error Details:")
                    st.code(task["error_message"])

elif page == "📈 Summary":
    st.title("📈 Summary")
    
    tasks = get_user_tasks(st.session_state.username)
    
    if tasks:
        completed = sum(1 for t in tasks if t["status"] == "completed")
        failed = sum(1 for t in tasks if t["status"] == "failed")
        in_progress = sum(1 for t in tasks if t["status"] in ["queued", "started"])
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total", len(tasks))
        col2.metric("✅ Completed", completed)
        col3.metric("🔄 In Progress", in_progress)
        col4.metric("❌ Failed", failed)
    else:
        st.info("No tasks to summarize yet!")
