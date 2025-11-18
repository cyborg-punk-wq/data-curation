import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client
import traceback
import json
import uuid

# Your original imports
import requests
from infiviz import Infiviz
from curation import Curation

# ===== SUPABASE CONNECTION =====
@st.cache_resource
def init_supabase():
    """Initialize Supabase client"""
    supabase_url = st.secrets["connections"]["supabase"]["SUPABASE_URL"]
    supabase_key = st.secrets["connections"]["supabase"]["SUPABASE_KEY"]
    return create_client(supabase_url, supabase_key)

supabase = init_supabase()

# ===== SECRETS =====
APIKEY = st.secrets["APIKEY"]
CURATION_TOKEN = st.secrets["CURATION_TOKEN"]
PROCESS_SESSION_URL_TEMPLATE = "https://aicontroller.infilect.com/processed_session/{}/?infiviz_session_id={}"
SOFTTAGS = ["brand", "variant", "sku"]

# ===== PAGE CONFIG =====
st.set_page_config(page_title="Session Processing Tool", page_icon="🔄", layout="wide")

# ===== AUTHENTICATION =====
if "email" not in st.user:
    st.error("🔒 Please log in with Google to access this application")
    st.info("Deploy this app on Streamlit Cloud with Google OAuth enabled")
    st.stop()

user_email = st.user.email
user_name = st.user.get("name", user_email.split("@")[0])

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

# ===== SUPABASE HELPERS =====
def create_task_in_db(task_id, user_email, params):
    """Create task record in Supabase"""
    supabase.table("tasks").insert({
        "task_id": task_id,
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
    """Update task status in Supabase"""
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

def get_user_tasks(user_email):
    """Get all tasks for a user"""
    response = supabase.table("tasks").select("*").eq("user_email", user_email).order("created_at", desc=True).execute()
    return response.data

# ===== TASK EXECUTION WRAPPER =====
def run_task(task_id, params):
    """Execute the task and track status"""
    try:
        # Update to started
        update_task_status(task_id, "started")
        
        # Step 1: Fetch sessions
        with st.status("**Step 1/3:** Fetching and sampling sessions...", expanded=True) as status:
            sampled_sessions = fetch_and_sample_sessions(
                params["client_id"],
                params["start_date"],
                params["end_date"],
                params["photo_types"],
                params["category_types"],
                params["channel_types"],
                params["sample_per_channel"]
            )
            st.write(f"✅ Found **{len(sampled_sessions)}** sessions")
            status.update(label=f"✅ Step 1/3: {len(sampled_sessions)} sessions found", state="complete")
        
        # Step 2: Download responses
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
            status.update(label=f"✅ Step 2/3: {len(responses)} responses downloaded", state="complete")
        
        # Step 3: Upload to curation
        with st.status("**Step 3/3:** Uploading to curation...", expanded=True) as status:
            progress_bar = st.progress(0)
            cur = Curation()
            cur.add_variables(params["dataset_id"], params["version_name"], CURATION_TOKEN, SOFTTAGS)
            
            success_count = 0
            for idx, resp in enumerate(responses):
                try:
                    cur.upload2curation(resp)
                    success_count += 1
                except Exception as e:
                    st.warning(f"Failed to upload session {resp.get('session_id')}")
                progress_bar.progress((idx + 1) / len(responses))
            
            st.write(f"✅ Uploaded **{success_count}/{len(responses)}** to curation")
            status.update(label=f"✅ Step 3/3: Upload complete", state="complete")
        
        # Mark as completed with summary
        summary = {
            "total_sessions": len(sampled_sessions),
            "successful_responses": len(responses),
            "uploaded_to_curation": success_count,
            "version_name": params["version_name"]
        }
        update_task_status(task_id, "completed", result_summary=summary)
        
        st.success(f"🎉 **Task completed successfully!**")
        st.json(summary)
        st.balloons()
        
        return True
        
    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        update_task_status(task_id, "failed", error_message=error_msg)
        st.error(f"❌ **Task failed:** {str(e)}")
        with st.expander("View full error"):
            st.code(error_msg)
        return False

# ===== SIDEBAR =====
with st.sidebar:
    st.markdown(f"### 👋 Welcome, {user_name}!")
    st.markdown(f"📧 {user_email}")
    st.markdown("---")
    page = st.radio("Navigation", ["🆕 New Task", "📊 My Tasks", "📈 Summary"], label_visibility="collapsed")

# ===== PAGE: NEW TASK =====
if page == "🆕 New Task":
    st.title("🆕 Create New Task")
    st.markdown("Fill in the parameters and click **Run Task** to start processing.")
    
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
        
        channel_types = st.text_area(
            "Channel Types (comma-separated)",
            value=(
                "sup-mkt-wh-supply,sml-grcry-upto-20-m2,self-service,supermarket,grocery,"
                "discount-stores,hypermarket,restcafebakery,stationers,large-grcry-41-100m2,"
                "horeca,med-grocry-21-40-m2,hypermkt-1001-m2-b,,hypermarket-1001-m2,"
                "hypermkt-1001-m2-a,minimktsse101-400m2,supermkt-401-1000-m2"
            ),
            height=80
        )
        
        submit = st.form_submit_button("▶️ Run Task", type="primary", use_container_width=True)
    
    if submit:
        # Generate version name if empty
        if not version_name:
            version_name = f"{category_types}_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}"
        
        # Generate task ID
        task_id = str(uuid.uuid4())
        
        # Prepare parameters
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
        
        # Create task in database
        create_task_in_db(task_id, user_email, params)
        
        st.markdown("---")
        st.markdown(f"### ⚙️ Processing Task: `{task_id}`")
        
        # Run task immediately (synchronously with progress)
        run_task(task_id, params)

# ===== PAGE: MY TASKS =====
elif page == "📊 My Tasks":
    st.title("📊 My Tasks")
    
    if st.button("🔄 Refresh", type="secondary"):
        st.rerun()
    
    tasks = get_user_tasks(user_email)
    
    if not tasks:
        st.info("No tasks found. Create your first task!")
    else:
        st.markdown(f"### Total Tasks: {len(tasks)}")
        
        for task in tasks:
            status_emoji = {
                "queued": "⏳",
                "started": "🔄",
                "completed": "✅",
                "failed": "❌"
            }.get(task["status"], "❓")
            
            with st.expander(
                f"{status_emoji} {task['version_name']} - **{task['status'].upper()}** "
                f"(Created: {task['created_at'][:19]})"
            ):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Task ID:** `{task['task_id']}`")
                    st.markdown(f"**Client:** {task['client_id']}")
                    st.markdown(f"**Category:** {task['category_types']}")
                    st.markdown(f"**Date Range:** {task['start_date']} to {task['end_date']}")
                
                with col2:
                    st.markdown(f"**Dataset ID:** {task['dataset_id']}")
                    st.markdown(f"**Status:** {task['status']}")
                    if task.get("started_at"):
                        st.markdown(f"**Started:** {task['started_at'][:19]}")
                    if task.get("completed_at"):
                        st.markdown(f"**Completed:** {task['completed_at'][:19]}")
                
                # Show summary if completed
                if task["status"] == "completed" and task.get("result_summary"):
                    st.markdown("---")
                    st.markdown("**📊 Results:**")
                    st.json(task["result_summary"])
                
                # Show errors if failed
                if task["status"] == "failed" and task.get("error_message"):
                    st.markdown("---")
                    st.error("**Error Details:**")
                    st.code(task["error_message"], language="text")

# ===== PAGE: SUMMARY =====
elif page == "📈 Summary":
    st.title("📈 Summary Dashboard")
    
    tasks = get_user_tasks(user_email)
    
    if not tasks:
        st.info("No tasks to summarize yet!")
    else:
        # Metrics
        completed = sum(1 for t in tasks if t["status"] == "completed")
        failed = sum(1 for t in tasks if t["status"] == "failed")
        in_progress = sum(1 for t in tasks if t["status"] in ["queued", "started"])
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Tasks", len(tasks))
        col2.metric("✅ Completed", completed)
        col3.metric("🔄 In Progress", in_progress)
        col4.metric("❌ Failed", failed)
        
        # Recent tasks table
        st.markdown("### Recent Tasks")
        df = pd.DataFrame(tasks)[["version_name", "status", "created_at", "completed_at"]].head(10)
        st.dataframe(df, use_container_width=True)
