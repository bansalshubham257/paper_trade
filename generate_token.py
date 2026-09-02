#!/usr/bin/env python3
"""
Upstox Token Generator

This script generates Upstox access tokens for multiple API keys and stores them in the database.
It's designed to be run as a daily scheduled task at 5 AM.

Usage:
  - Run manually: python generate_token.py
  - Schedule with cron: 0 5 * * * cd /path/to/option-chain-python && python generate_token.py

Options:
  --visible: Run in visible browser mode (for debugging only)
  --manual: Use manual authentication flow instead of automated
  --add-account: Add a new Upstox account to the database
"""

import argparse
import os
import getpass
import sys
from datetime import datetime
from upstox_auth import automated_auth_flow, manual_auth_flow, fully_automated_auth_flow
from dotenv import load_dotenv
from config import Config
from services.database import DatabaseService

# Initialize database service
db_service = DatabaseService()

def get_account_details():
    """Prompt the user for account details"""
    print("\n=== Add Upstox Account ===")
    api_key = input("API Key: ")
    api_secret = input("API Secret: ")
    totp_secret = input("TOTP Secret: ")
    redirect_uri = input("Redirect URI [http://localhost:5000/callback]: ") or "http://localhost:5000/callback"
    username = input("Upstox Username: ")
    password = getpass.getpass("Upstox Password: ")

    return {
        'api_key': api_key,
        'api_secret': api_secret,
        'totp_secret': totp_secret,
        'redirect_uri': redirect_uri,
        'username': username,
        'password': password,
        'is_active': True
    }

def add_account():
    """Add a new account to the database"""
    account_data = get_account_details()

    if db_service.save_upstox_account(account_data):
        print("Account saved successfully!")
        return True
    else:
        print("Failed to save account.")
        return False

def generate_token_for_account(account, headless=True, manual=False):
    """Generate a token for a specific account"""
    api_key = account.get('api_key')
    api_secret = account.get('api_secret')
    totp_secret = account.get('totp_secret')
    redirect_uri = account.get('redirect_uri')
    username = account.get('username')
    password = account.get('password')

    if not all([api_key, api_secret, totp_secret, redirect_uri]):
        print(f"Missing required credentials for account {api_key}")
        return False

    print(f"Generating token for account {api_key}")

    # Check if using manual flow
    if manual:
        print(f"Using manual flow for account {api_key}")
        access_token = manual_auth_flow(api_key, api_secret, totp_secret, redirect_uri)
    else:
        # Check if we have username/password for fully automated flow
        if username and password:
            print(f"Using fully automated flow for account {api_key}")
            access_token = fully_automated_auth_flow(
                api_key, api_secret, totp_secret, redirect_uri,
                headless=headless, username=username, password=password
            )
        else:
            print(f"Using semi-automated flow for account {api_key}")
            access_token = automated_auth_flow(api_key, api_secret, totp_secret, redirect_uri)

    # Check if token generation was successful
    if access_token:
        print(f"Token generation successful for account {api_key}")

        # Create token data structure with required fields
        token_data = {
            'access_token': access_token,
            'created_at': datetime.now().timestamp()
        }

        # Update token directly in database
        if db_service.update_upstox_token(api_key, token_data):
            print(f"Token updated in database for account {api_key}")
            return True
        else:
            print(f"Failed to update token in database for account {api_key}")
    else:
        print(f"Token generation failed for account {api_key}")

    return False

def main():
    parser = argparse.ArgumentParser(description='Generate Upstox access tokens for multiple accounts')
    parser.add_argument('--visible', action='store_true', help='Run browser in visible mode (for debugging)')
    parser.add_argument('--manual', action='store_true', help='Use manual flow instead of automated')
    parser.add_argument('--add-account', action='store_true', help='Add a new Upstox account to the database')
    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Add account if requested
    if args.add_account:
        if add_account():
            print("New account added successfully")
        else:
            print("Failed to add new account")
            sys.exit(1)
        return

    # Get accounts from database
    accounts = db_service.get_upstox_accounts()

    if not accounts:
        print("No accounts found in database")
        print("No accounts found. Use --add-account to add a new account.")
        sys.exit(1)

    # Generate tokens for each account
    success_count = 0
    for account in accounts:
        if generate_token_for_account(account, headless=not args.visible, manual=args.manual):
            success_count += 1

    # Log summary
    print(f"Token generation complete. Generated {success_count}/{len(accounts)} tokens successfully.")

    # Exit with appropriate code
    sys.exit(0 if success_count == len(accounts) else 1)

if __name__ == "__main__":
    main()
