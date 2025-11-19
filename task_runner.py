import sys
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from datetime import datetime
from supabase import create_client
import requests

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

# ===== MAIN TASK EXECUTION =====
def execute_task(task_id, params):
    """Execute the actual task logic"""
    try:
        logger.info(f"Starting task execution: {task_id}")
        logger.info(f"Parameters: {json.dumps(params, default=str)}")
        
        # Mark as started
        update_task_status(task_id, "started", started_at=datetime.now().isoformat())
        
        # Import your processing modules
        from infiviz import InfiViz
        from curation import CurationTool
        
        # Initialize tools
        infiviz = InfiViz(api_key=APIKEY)
        curation = CurationTool(token=CURATION_TOKEN)
        
        # Check cancellation before starting
        if is_task_cancelled(task_id):
            logger.warning(f"Task {task_id} was cancelled before processing")
            return
        
        # Step 1: Fetch session data
        logger.info(f"Fetching sessions for task {task_id}")
        sessions = infiviz.get_sessions(
            client_id=params["client_id"],
            start_date=params["start_date"],
            end_date=params["end_date"],
            photo_types=params["photo_types"],
            category_types=params["category_types"],
            channel_types=params["channel_types"]
        )
        
        if is_task_cancelled(task_id):
            logger.warning(f"Task {task_id} cancelled after fetching sessions")
            return
        
        logger.info(f"Fetched {len(sessions)} sessions")
        
        # Step 2: Sample sessions
        logger.info(f"Sampling sessions for task {task_id}")
        sampled_sessions = infiviz.sample_sessions(
            sessions,
            sample_per_channel=params["sample_per_channel"]
        )
        
        if is_task_cancelled(task_id):
            logger.warning(f"Task {task_id} cancelled after sampling")
            return
        
        logger.info(f"Sampled {len(sampled_sessions)} sessions")
        
        # Step 3: Process and upload
        logger.info(f"Processing images for task {task_id}")
        processed_data = infiviz.process_images(sampled_sessions)
        
        if is_task_cancelled(task_id):
            logger.warning(f"Task {task_id} cancelled after processing")
            return
        
        logger.info(f"Uploading to curation tool for task {task_id}")
        upload_result = curation.upload_dataset(
            dataset_id=params["dataset_id"],
            version_name=params["version_name"],
            data=processed_data
        )
        
        if is_task_cancelled(task_id):
            logger.warning(f"Task {task_id} cancelled after upload")
            return
        
        # Create result summary
        result_summary = {
            "total_sessions": len(sessions),
            "sampled_sessions": len(sampled_sessions),
            "processed_images": len(processed_data),
            "upload_status": upload_result.get("status", "unknown"),
            "dataset_id": params["dataset_id"],
            "version_name": params["version_name"]
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
        logger.error(f"Task {task_id} failed: {str(e)}", exc_info=True)
        update_task_status(
            task_id,
            "failed",
            completed_at=datetime.now().isoformat(),
            error_message=str(e)
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
