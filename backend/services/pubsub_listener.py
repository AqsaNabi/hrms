import json
import os
import threading
import time

import tempfile
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError

from backend.services.applicant_response_service import (
    process_form_responses
)

PROJECT_ID = "manifest-life-513710-d4"
SUBSCRIPTION_ID = "nexora-form-responses-sub"

SUBSCRIPTION_PATH = (
    f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
)

subscriber = None

_listener_thread = None
_stop_event = threading.Event()



def _get_subscriber():
    global subscriber

    if subscriber is None:
        # ✅ FIXED: Read from environment variable string
        env_creds_json = os.getenv("GOOGLE_PUBSUB_CREDENTIALS_JSON")
        
        if env_creds_json:
            # Write to temp file
            temp_path = os.path.join(tempfile.gettempdir(), "pubsub_creds.json")
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(env_creds_json)
                
            credentials = service_account.Credentials.from_service_account_file(temp_path)
            subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            
        else:
            # Original fallback logic
            creds_path = os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS", 
                "backend/authen/pubsub_credentials.json"
            )
            
            if os.path.exists(creds_path):
                credentials = service_account.Credentials.from_service_account_file(creds_path)
                subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
            else:
                subscriber = pubsub_v1.SubscriberClient()

    return subscriber

def start_pubsub_listener():
    global _listener_thread

    if _listener_thread and _listener_thread.is_alive():
        print("Pub/Sub listener is already running.")
        return

    _stop_event.clear()

    _listener_thread = threading.Thread(
        target=_listen,
        daemon=True
    )

    _listener_thread.start()


def stop_pubsub_listener():
    global subscriber

    _stop_event.set()

    if _listener_thread and _listener_thread.is_alive():
        _listener_thread.join(timeout=5)

    if subscriber is not None:
        subscriber.close()
        subscriber = None
