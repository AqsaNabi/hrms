import tempfile
import json
import os
from pathlib import Path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

BASE_DIR = Path(__file__).resolve().parents[1]

# ✅ 1. Prioritize checking for environment variable tokens
env_token_json = os.getenv("GOOGLE_TOKEN_JSON")
if env_token_json:
    # Write to Render's temporary directory
    TOKEN_FILE = Path(tempfile.gettempdir()) / "token.json"
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        f.write(env_token_json)
else:
    # ✅ 2. Fallback to local path logic
    TOKEN_FILE = Path(
        os.getenv("GOOGLE_TOKEN_PATH", str(BASE_DIR / "authen" / "token.json"))
    )

# ... your original FORMS_SCOPE ...

def get_credentials():
    if not TOKEN_FILE.exists():
        raise FileNotFoundError(f"Token not found at {TOKEN_FILE}. Set GOOGLE_TOKEN_JSON on Render.")

    credentials = Credentials.from_authorized_user_file(
        str(TOKEN_FILE),
        scopes=FORMS_SCOPE
    )

    if credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
        try:
            TOKEN_FILE.write_text(credentials.to_json(), encoding="utf-8")
        except OSError:
            pass

    if not credentials.valid:
        raise ValueError("Google credentials are invalid.")

    return credentials

# ... the rest remains unchanged ...
