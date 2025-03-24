from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

def get_env_bool(key: str, default: bool = False) -> bool:
    """Get a boolean environment variable."""
    value = os.environ.get(key)
    if value is None:
        return default
    return value.lower() in ('true', '1', 't', 'y', 'yes') 