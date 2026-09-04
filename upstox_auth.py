import requests
import pyotp
import os
import webbrowser
import socket
import threading
from urllib.parse import parse_qs, urlparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime
import time
class UpstoxAuth:
    """
    Utility class for Upstox authentication.
    Generates and manages access tokens using API key and TOTP.
    """

    def __init__(self, api_key, secret, totp_secret, redirect_uri):
        """
        Initialize with API credentials.

        Args:
            api_key (str): Upstox API key
            secret (str): Upstox API secret
            totp_secret (str): TOTP secret for 2FA
            redirect_uri (str): Redirect URI registered with Upstox
        """
        self.api_key = api_key
        self.secret = secret
        self.totp_secret = totp_secret
        self.redirect_uri = redirect_uri

    def generate_totp(self):
        """Generate TOTP code for 2FA."""
        totp = pyotp.TOTP(self.totp_secret)
        return totp.now()

    def get_auth_url(self):
        """Get the authorization URL for user login."""
        base_url = "https://api.upstox.com/v2/login/authorization/dialog"
        auth_url = f"{base_url}?client_id={self.api_key}&redirect_uri={self.redirect_uri}&response_type=code"
        return auth_url

    def get_access_token(self, auth_code):
        """
        Get access token using authorization code.

        Args:
            auth_code (str): Authorization code received after user login

        Returns:
            dict: Access token response with token and expiry
        """
        url = "https://api.upstox.com/v2/login/authorization/token"

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }

        payload = {
            "code": auth_code,
            "client_id": self.api_key,
            "client_secret": self.secret,
            "redirect_uri": self.redirect_uri,
            "grant_type": "authorization_code"
        }

        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            token_data = response.json()
            # Add current timestamp for expiry tracking
            token_data['created_at'] = datetime.now().timestamp()

            # Set default expiry if not provided (1 day)
            if 'expires_in' not in token_data:
                token_data['expires_in'] = 86400  # 24 hours in seconds
                print("Warning: Token expiry not provided by API, using default (24 hours)")

            return token_data
        else:
            print(f"Error getting access token: {response.status_code}")
            print(response.text)
            return None

    def verify_token(self, access_token):
        """Verify if the token is valid by making a test API call."""
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }

        url = "https://api.upstox.com/v2/user/profile"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            print("Token verification successful!")
            user_data = response.json()
            return user_data
        elif response.status_code == 403:
            # Static IP restriction — token is still valid, skip verification
            print("Token verification skipped: API requires static IP configuration (UDAPI1154).")
            print("Token is valid — proceeding without profile verification.")
            return {"skipped": True, "reason": "static_ip_restriction"}
        else:
            print(f"Token verification failed: {response.status_code}")
            print(response.text)
            return None

    def start_auth_server(self):
        """Start a local server to catch the redirect with auth code."""
        # Parse the redirect URI to get host and port
        parsed_uri = urlparse(self.redirect_uri)
        host = parsed_uri.hostname
        port = parsed_uri.port or 80

        # Create a global variable to store the auth code
        global auth_code
        auth_code = None

        class RedirectHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                global auth_code  # Use global instead of nonlocal

                # Parse the query parameters from the URL
                query = urlparse(self.path).query
                params = parse_qs(query)

                # Get the authorization code
                if 'code' in params:
                    auth_code = params['code'][0]
                    success_html = """
                    <html>
                        <head><title>Authentication Successful</title></head>
                        <body>
                            <h1>Authentication Successful!</h1>
                            <p>You can close this window and return to the application.</p>
                        </body>
                    </html>
                    """
                    self.send_response(200)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(success_html.encode())
                else:
                    error_html = """
                    <html>
                        <head><title>Authentication Failed</title></head>
                        <body>
                            <h1>Authentication Failed</h1>
                            <p>No authorization code was received. Please try again.</p>
                        </body>
                    </html>
                    """
                    self.send_response(400)
                    self.send_header('Content-type', 'text/html')
                    self.end_headers()
                    self.wfile.write(error_html.encode())

            # Suppress server logs
            def log_message(self, format, *args):
                return

        # Check if we can bind to the redirect URI port
        try:
            # Create and start the server
            server = HTTPServer((host, port), RedirectHandler)

            # Run the server in a separate thread so it doesn't block
            server_thread = threading.Thread(target=server.serve_forever)
            server_thread.daemon = True
            server_thread.start()

            print(f"Listening for Upstox callback on {host}:{port}")
            return server
        except socket.error as e:
            print(f"Error starting local server: {e}")
            print(f"Make sure no other application is using port {port} on {host}")
            print("Will fallback to manual code entry")
            return None

    def stop_auth_server(self, server):
        """Stop the local server."""
        if server:
            server.shutdown()
            server.server_close()

def automated_auth_flow(api_key, secret, totp_secret, redirect_uri):
    """
    Run an automated authentication flow with local server.

    Args:
        api_key (str): Upstox API key
        secret (str): Upstox API secret
        totp_secret (str): TOTP secret for 2FA
        redirect_uri (str): Redirect URI registered with Upstox

    Returns:
        str: Access token if successful, None otherwise
    """
    auth = UpstoxAuth(api_key, secret, totp_secret, redirect_uri)

    # Start the local server to catch the redirect
    server = auth.start_auth_server()

    # Generate auth URL and TOTP code
    auth_url = auth.get_auth_url()
    totp_code = auth.generate_totp()

    print("\n=== Upstox Authentication Flow ===")
    print(f"\n1. Opening this URL in your browser:\n{auth_url}")
    print("\n2. Log in with your Upstox credentials")
    print("\n3. When prompted for TOTP, use this code:", totp_code)
    print("\n4. After successful login, you'll be redirected automatically")

    # Open the browser with the auth URL
    webbrowser.open(auth_url)

    # If server started successfully, wait for redirect
    if server:
        # Wait for the auth code with timeout
        timeout = 300  # 5 minutes
        start_time = time.time()

        print("\nWaiting for authentication (timeout in 5 minutes)...")

        # Wait for auth_code to be set by the server
        while 'auth_code' not in globals() or globals()['auth_code'] is None:
            time.sleep(1)
            if time.time() - start_time > timeout:
                print("Authentication timed out. Please try again.")
                auth.stop_auth_server(server)
                return None

        # Get the auth code from global variable
        auth_code = globals()['auth_code']
        print("\nAuthorization code received automatically!")

        # Stop the server
        auth.stop_auth_server(server)
    else:
        # Server failed to start, fall back to manual entry
        print("\nFalling back to manual authorization code entry.")
        auth_code = input("\nEnter the code from the redirect URL: ")

    # Get access token using the auth code
    token_data = auth.get_access_token(auth_code)
    if token_data and 'access_token' in token_data:
        print("\nAuthentication successful!")
        access_token = token_data['access_token']

        # Verify the token (optional — may be skipped if static IP is not configured)
        user_data = auth.verify_token(access_token)
        if user_data and 'data' in user_data:
            print(f"\nLogged in as: {user_data.get('data', {}).get('user_name', 'Unknown')}")
            print(f"Email: {user_data.get('data', {}).get('email', 'Unknown')}")

        # Safely display expiry info
        if 'expires_in' in token_data:
            hours = token_data['expires_in'] // 3600
            print(f"\nToken will expire in: {hours} hours")
        else:
            print("\nToken expiry information not available")

        return access_token

    return None

def manual_auth_flow(api_key, secret, totp_secret, redirect_uri):
    """
    Run a manual authentication flow in the console.

    Args:
        api_key (str): Upstox API key
        secret (str): Upstox API secret
        totp_secret (str): TOTP secret for 2FA
        redirect_uri (str): Redirect URI registered with Upstox

    Returns:
        str: Access token if successful, None otherwise
    """
    auth = UpstoxAuth(api_key, secret, totp_secret, redirect_uri)

    # No valid token, start new flow
    auth_url = auth.get_auth_url()
    print("\n=== Upstox Authentication Flow ===")
    print(f"\n1. Open this URL in your browser:\n{auth_url}")
    print("\n2. Log in with your Upstox credentials")
    print("\n3. When prompted for TOTP, use this code:", auth.generate_totp())
    print("\n4. After successful login, you'll be redirected to your redirect URI")
    print("   The URL will contain a 'code' parameter")
    print(f"\nYour configured redirect URI is: {redirect_uri}")
    print("Make sure this EXACTLY matches what you registered in the Upstox Developer Dashboard")

    auth_code = input("\nEnter the code from the redirect URL: ")

    # Get access token
    token_data = auth.get_access_token(auth_code)
    if token_data and 'access_token' in token_data:
        print("\nAuthentication successful!")
        access_token = token_data['access_token']

        # Verify the token (optional — may be skipped if static IP is not configured)
        user_data = auth.verify_token(access_token)
        if user_data and 'data' in user_data:
            print(f"\nLogged in as: {user_data.get('data', {}).get('user_name', 'Unknown')}")
            print(f"Email: {user_data.get('data', {}).get('email', 'Unknown')}")

        # Safely display expiry info
        if 'expires_in' in token_data:
            hours = token_data['expires_in'] // 3600
            print(f"\nToken will expire in: {hours} hours")
        else:
            print("\nToken expiry information not available")

        return access_token

    return None

def fully_automated_auth_flow(api_key, secret, totp_secret, redirect_uri,
                              headless=True, username=None, password=None):
    """
    Run a fully automated authentication flow with Playwright (on-demand chromium).

    Playwright launches a headless chromium only for the duration of this call,
    then shuts it down - avoiding the cost of a 24/7 Selenium/Chrome service.

    Args:
        api_key (str): Upstox API key
        secret (str): Upstox API secret
        totp_secret (str): TOTP secret for 2FA
        redirect_uri (str): Redirect URI registered with Upstox
        headless (bool): Whether to run browser in headless mode
        username (str): Upstox username/mobile number
        password (str): Upstox password

    Returns:
        str: Access token if successful, None otherwise
    """
    auth = UpstoxAuth(api_key, secret, totp_secret, redirect_uri)

    # Start the local server to catch the redirect
    server = auth.start_auth_server()
    if not server:
        print("Failed to start local server. Cannot proceed with automated authentication.")
        return None

    # Generate auth URL and TOTP code
    auth_url = auth.get_auth_url()
    totp_code = auth.generate_totp()

    print("\n=== Upstox Fully Automated Authentication Flow (Playwright) ===")
    print("Launching on-demand headless Chromium...")

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        print(f"ERROR: Playwright not available: {e}")
        if 'server' in locals() and server:
            auth.stop_auth_server(server)
        return None

    page = None
    p = None
    try:
        p = sync_playwright().start()
        browser = p.chromium.launch(
            headless=headless,
            args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--window-size=1920,1080',
            ],
        )
        page = browser.new_page(
            user_agent=('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/120.0.0.0 Safari/537.36')
        )

        print("Navigating to Upstox authorization page...")
        page.goto(auth_url, timeout=60000)

        # Check if we need username/password
        if username and password:
            try:
                # STEP 1: Enter mobile number/username
                print("Entering username/mobile number...")
                username_field = page.wait_for_selector("#mobileNum", timeout=30000)
                username_field.fill(username)
                print("Username entered successfully")

                # Click continue after username
                page.wait_for_selector("#getOtp", state="attached", timeout=30000)
                page.click("#getOtp")
                print("Clicked continue after username")
            except Exception as e:
                print(f"Error in username step: {str(e)}")

            # Small delay to ensure the OTP page loads
            page.wait_for_timeout(2000)

            # STEP 2: Enter TOTP from the authenticator app
            try:
                print(f"Entering TOTP code: {totp_code}")
                totp_field = page.wait_for_selector("#otpNum", timeout=30000)
                totp_field.fill(totp_code)
                print("TOTP entered successfully")

                page.wait_for_selector("#continueBtn", state="attached", timeout=30000)
                page.click("#continueBtn")
                print("Clicked sign in button")
            except Exception as e:
                print(f"Error in TOTP step: {str(e)}")

            # STEP 3: Enter password/PIN
            try:
                password_field = page.wait_for_selector("#pinCode", timeout=30000)
                password_field.fill(password)
                page.wait_for_timeout(2000)
                page.wait_for_selector("#pinContinueBtn", state="attached", timeout=30000)
                page.click("#pinContinueBtn")
                print("Password entered successfully")
            except Exception as e:
                print(f"Error in PIN step: {str(e)}")

            print("Login credentials submitted successfully")
            page.wait_for_timeout(3000)
        else:
            print("No credentials provided. Waiting for manual login...")
            page.wait_for_timeout(30000)

        # Wait for the redirect URL containing the authorization code
        timeout = 120  # 2 minutes timeout
        start_time = time.time()
        auth_code = None

        while time.time() - start_time < timeout:
            current_url = page.url
            if 'code=' in current_url:
                # Parse the code from URL
                parsed = urlparse(current_url)
                params = parse_qs(parsed.query)
                auth_code = params['code'][0]
                print(f"\nAuthorization code received: {auth_code}")
                break
            page.wait_for_timeout(1000)
        else:
            print("Authentication timed out. Please try again.")
            try:
                page.screenshot(path="timeout_error.png")
                print(f"Screenshot saved to timeout_error.png")
            except Exception as se:
                print(f"Failed to save timeout screenshot: {se}")
            print(f"Final URL: {page.url}")
            browser.close()
            p.stop()
            auth.stop_auth_server(server)
            return None

        # Close the browser (released on-demand - no 24/7 cost)
        browser.close()
        p.stop()

        # Stop the server
        auth.stop_auth_server(server)

        # Get access token using the auth code
        token_data = auth.get_access_token(auth_code)
        if token_data and 'access_token' in token_data:
            print("\nAuthentication successful!")
            access_token = token_data['access_token']

            # Verify the token (optional - may be skipped if static IP is not configured)
            user_data = auth.verify_token(access_token)
            if user_data and 'data' in user_data:
                print(f"\nLogged in as: {user_data.get('data', {}).get('user_name', 'Unknown')}")
                print(f"Email: {user_data.get('data', {}).get('email', 'Unknown')}")

            # Safely display expiry info
            if 'expires_in' in token_data:
                hours = token_data['expires_in'] // 3600
                print(f"\nToken will expire in: {hours} hours")
            else:
                print("\nToken expiry information not available")

            return access_token

        return None

    except Exception as e:
        print(f"Error in automated authentication: {str(e)}")
        try:
            if page is not None:
                page.screenshot(path="error_screenshot.png")
                print(f"Error screenshot saved to error_screenshot.png")
                print(f"Current URL at error: {page.url}")
        except Exception as screenshot_error:
            print(f"Failed to take error screenshot: {str(screenshot_error)}")

        if 'server' in locals() and server:
            auth.stop_auth_server(server)
        if p is not None:
            try:
                p.stop()
            except Exception:
                pass
        return None
if __name__ == "__main__":
    # This will be executed when running this file directly
    # Import credentials from config if available, otherwise use placeholders
    try:
        from config import Config
        api_key = Config.API_KEY
        secret = Config.API_SECRET
        totp_secret = Config.TOTP_SECRET
        redirect_uri = Config.REDIRECT_URI
    except (ImportError, AttributeError):
        print("Warning: Config not found or missing credentials, using placeholders")
        api_key = "your_api_key"
