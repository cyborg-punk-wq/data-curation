import sys
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
from supabase import create_client
import requests
import traceback

# ===== LOGGING SETUP =====
def setup_logging():
    """Setup proper logging for background task"""
    os.makedirs('logs', exist_ok=True)
    
    logger = logging.getLogger('task_runner')
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    file_handler = RotatingFileHandler(
        'logs/task_runner.log',
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


# ===== LOAD SECRETS =====
def load_secrets():
    """Load secrets from Streamlit secrets file"""
    import toml
    secrets_path = os.path.join('.streamlit', 'secrets.toml')
    with open(secrets_path, 'r') as f:
        secrets = toml.load(f)
    return secrets


secrets = load_secrets()
supabase = create_client(
    secrets['connections']['supabase']['SUPABASE_URL'],
    secrets['connections']['supabase']['SUPABASE_KEY']
)
APIKEY = secrets['APIKEY']
CURATION_TOKEN = secrets['CURATION_TOKEN']

# Constants
PROCESS_SESSION_URL_TEMPLATE = "https://aicontroller.infilect.com/processed_session/{}/?infiviz_session_id={}"
SOFTTAGS = ["brand", "variant", "sku"]


# ===== TASK STATUS UPDATES =====
def update_task_status(task_id, status, **kwargs):
    """Update task status in database"""
    try:
        update_data = {"status": status}
        update_data.update(kwargs)
        supabase.table("tasks").update(update_data).eq("task_id", task_id).execute()
        logger.info(f"Task {task_id} status updated to: {status}")
    except Exception as e:
        logger.error(f"Error updating task {task_id} status: {str(e)}", exc_info=True)


def is_task_cancelled(task_id):
    """Check if task has been cancelled"""
    try:
        response = supabase.table("tasks").select("status").eq("task_id", task_id).execute()
        if response.data and response.data[0]['status'] == 'cancelled':
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking cancellation for {task_id}: {str(e)}")
        return False


# ===== STEP 1: FETCH & SAMPLE SESSIONS =====
def fetch_and_sample_sessions(task_id, client_id, category_types, channel_types, 
                                photo_types, end_date, start_date, sample_per_channel):
    """Fetch and sample sessions from Infiviz"""
    try:
        from infiviz import Infiviz
        
        logger.info(f"Task {task_id}: Initializing Infiviz")
        inf = Infiviz()
        inf.add_variables(client_id, category_types, channel_types, photo_types, end_date, start_date)
        
        logger.info(f"Task {task_id}: Fetching combinations from Infiviz")
        inf.get_combinations(processed=True)
        
        all_data = inf.all_sessions
        logger.info(f"Task {task_id}: Total sessions found: {len(all_data)}")
        
        # Check cancellation
        if is_task_cancelled(task_id):
            logger.warning(f"Task {task_id}: Cancelled after fetching sessions")
            return None
        
        logger.info(f"Task {task_id}: Returning {len(all_data)} sessions without additional sampling")
        return all_data
        
    except Exception as e:
        logger.error(f"Task {task_id}: Error in fetch_and_sample_sessions: {str(e)}", exc_info=True)
        raise


# ===== STEP 2: DOWNLOAD PROCESSED OUTPUTS =====
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


def download_responses(task_id, sampled_sessions, client_id):
    """Download processed outputs for all sampled sessions"""
    session_ids = [s["session_id"] for s in sampled_sessions]
    logger.info(f"Task {task_id}: Downloading processed outputs for {len(session_ids)} sessions")
    
    responses = []
    failed_count = 0
    
    for idx, sess_id in enumerate(session_ids):
        # Check cancellation every 10 sessions
        if idx % 10 == 0 and is_task_cancelled(task_id):
            logger.warning(f"Task {task_id}: Cancelled after downloading {idx}/{len(session_ids)} sessions")
            return None
        
        resp = fetch_output_from_ai_controller(sess_id, client_id)
        if resp:
            responses.append(resp)
        else:
            failed_count += 1
        
        # Log progress every 50 sessions
        if (idx + 1) % 50 == 0:
            logger.info(f"Task {task_id}: Downloaded {idx + 1}/{len(session_ids)} sessions")
    
    logger.info(f"Task {task_id}: Total successful responses: {len(responses)}, failed: {failed_count}")
    return responses


# ===== STEP 3: UPLOAD TO CURATION =====
def upload_to_curation(task_id, responses, dataset_id, version_name):
    """Upload responses to Curation tool"""
    try:
        from curation import Curation
        
        logger.info(f"Task {task_id}: Initializing Curation tool")
        cur = Curation()
        cur.add_variables(dataset_id, version_name, CURATION_TOKEN, SOFTTAGS)
        
        logger.info(f"Task {task_id}: Uploading {len(responses)} responses to Curation")
        
        success_count = 0
        failed_sessions = []
        
        for idx, resp in enumerate(responses):
            # Check cancellation every 10 uploads
            if idx % 10 == 0 and is_task_cancelled(task_id):
                logger.warning(f"Task {task_id}: Cancelled after uploading {success_count}/{len(responses)} sessions")
                return None
            
            try:
                cur.upload2curation(resp)
                success_count += 1
            except Exception as e:
                session_id = resp.get('session_id', 'unknown')
                failed_sessions.append(session_id)
                logger.error(f"Task {task_id}: Failed to upload session {session_id}: {str(e)}")
            
            # Log progress every 50 uploads
            if (idx + 1) % 50 == 0:
                logger.info(f"Task {task_id}: Uploaded {idx + 1}/{len(responses)} sessions")
        
        logger.info(f"Task {task_id}: Upload complete - {success_count} successful, {len(failed_sessions)} failed")
        
        return {
            "success_count": success_count,
            "failed_sessions": failed_sessions
        }
        
    except Exception as e:
        logger.error(f"Task {task_id}: Error in upload_to_curation: {str(e)}", exc_info=True)
        raise


# ===== MAIN TASK EXECUTION =====
def execute_task(task_id, params):
    """Execute the actual task logic"""
    try:
        logger.info(f"Starting task execution: {task_id}")
        logger.info(f"Parameters: {json.dumps(params, default=str)}")
        
        # Mark as started
        update_task_status(task_id, "started", started_at=datetime.now().isoformat())
        
        # Check cancellation before starting
        if is_task_cancelled(task_id):
            logger.warning(f"Task {task_id} was cancelled before processing")
            return
        
        # Step 1: Fetch and sample sessions
        logger.info(f"Task {task_id}: Step 1 - Fetching and sampling sessions")
        sampled_sessions = fetch_and_sample_sessions(
            task_id,
            params["client_id"],
            params["category_types"],
            params["channel_types"],
            params["photo_types"],
            params["end_date"],
            params["start_date"],
            params["sample_per_channel"]
        )
        
        if sampled_sessions is None:
            logger.warning(f"Task {task_id}: Cancelled during session fetching")
            return
        
        if len(sampled_sessions) == 0:
            error_msg = "No sessions found matching the criteria"
            logger.error(f"Task {task_id}: {error_msg}")
            update_task_status(task_id, "failed", 
                             completed_at=datetime.now().isoformat(),
                             error_message=error_msg)
            return
        
        # Step 2: Download responses
        logger.info(f"Task {task_id}: Step 2 - Downloading processed outputs")
        responses = download_responses(task_id, sampled_sessions, params["client_id"])
        
        if responses is None:
            logger.warning(f"Task {task_id}: Cancelled during response download")
            return
        
        if len(responses) == 0:
            error_msg = "No valid responses downloaded from AI controller"
            logger.error(f"Task {task_id}: {error_msg}")
            update_task_status(task_id, "failed",
                             completed_at=datetime.now().isoformat(),
                             error_message=error_msg)
            return
        
        # Step 3: Upload to curation
        logger.info(f"Task {task_id}: Step 3 - Uploading to curation")
        upload_result = upload_to_curation(
            task_id,
            responses,
            params["dataset_id"],
            params["version_name"]
        )
        
        if upload_result is None:
            logger.warning(f"Task {task_id}: Cancelled during upload")
            return
        
        # Create result summary
        result_summary = {
            "total_sessions": len(sampled_sessions),
            "downloaded_responses": len(responses),
            "uploaded_successfully": upload_result["success_count"],
            "failed_uploads": len(upload_result["failed_sessions"]),
            "dataset_id": params["dataset_id"],
            "version_name": params["version_name"],
            "client_id": params["client_id"]
        }
        
        # Mark as completed
        update_task_status(
            task_id,
            "completed",
            completed_at=datetime.now().isoformat(),
            result_summary=result_summary
        )
        
        logger.info(f"Task {task_id} completed successfully")
        logger.info(f"Result: {json.dumps(result_summary, indent=2)}")
        
    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        logger.error(f"Task {task_id} failed: {error_msg}")
        update_task_status(
            task_id,
            "failed",
            completed_at=datetime.now().isoformat(),
            error_message=error_msg
        )


def main():
    """Main entry point for background task runner"""
    if len(sys.argv) != 3:
        print("Usage: python task_runner.py <task_id> <params_file>")
        sys.exit(1)
    
    task_id = sys.argv[1]
    params_file = sys.argv[2]
    
    logger.info(f"Task runner started for task: {task_id}")
    logger.info(f"Loading parameters from: {params_file}")
    
    try:
        # Load parameters
        with open(params_file, 'r') as f:
            params = json.load(f)
        
        # Execute task
        execute_task(task_id, params)
        
        # Clean up params file
        try:
            os.remove(params_file)
            logger.info(f"Cleaned up params file: {params_file}")
        except Exception as e:
            logger.warning(f"Could not remove params file: {e}")
            
    except Exception as e:
        logger.error(f"Fatal error in task runner: {str(e)}", exc_info=True)
        sys.exit(1)
    
    logger.info(f"Task runner finished for task: {task_id}")


if __name__ == "__main__":
    main()
