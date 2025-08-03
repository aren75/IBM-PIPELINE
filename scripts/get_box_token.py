# get_box_tokens.py

import os
from dotenv import load_dotenv
from boxsdk import OAuth2, Client

# Load environment variables from .env file
load_dotenv()

BOX_CLIENT_ID = os.getenv('BOX_CLIENT_ID')
BOX_CLIENT_SECRET = os.getenv('BOX_CLIENT_SECRET')

if not BOX_CLIENT_ID or not BOX_CLIENT_SECRET:
    print("ERROR: BOX_CLIENT_ID or BOX_CLIENT_SECRET is missing from .env.")
    print("Please ensure they are set from your Box Custom App configuration.")
    exit(1)

# Initialize OAuth2 object without tokens
# The redirect_uri must match what you configured in your Box App settings
redirect_uri = 'http://localhost' # MUST MATCH the Redirect URI in your Box App config
oauth = OAuth2(
    client_id=BOX_CLIENT_ID,
    client_secret=BOX_CLIENT_SECRET,
    access_token=None,  # No initial access token
    refresh_token=None, # No initial refresh token
    store_tokens=None,  # We will handle storage manually for the refresh token
)

# Step 1: Get the authorization URL
auth_url, csrf_token = oauth.get_authorization_url(redirect_uri)
print(f"Please open this URL in your web browser to authorize your application:")
print(auth_url)

# Step 2: User opens URL, grants access, and is redirected to localhost
# The browser will try to open http://localhost/?code=YOUR_CODE&state=YOUR_STATE
# You need to manually copy the 'code' query parameter from the URL.
authorization_code = input("\nAfter authorizing, copy the 'code' parameter from the redirected URL (e.g., http://localhost/?code=YOUR_CODE&state=...): ")

try:
    # Step 3: Exchange the authorization code for tokens
    access_token, refresh_token = oauth.authenticate(authorization_code)

    print("\n--- SUCCESSFULLY OBTAINED TOKENS ---")
    print(f"Access Token: {access_token}")
    print(f"Refresh Token: {refresh_token}")
    print("\nIMPORTANT: Add this line to your .env file (replace existing if any):")
    print(f"BOX_REFRESH_TOKEN='{refresh_token}'") # Added quotes for string safety
    print("\nAfter adding, you can delete this script (get_box_tokens.py).")

except Exception as e:
    print(f"\nERROR: Failed to obtain tokens. Please check your Client ID, Client Secret, and the authorization code you pasted.")
    print(f"Error details: {e}")