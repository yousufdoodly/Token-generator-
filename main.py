import json
import os
import sys
import time
import requests
import base64
from datetime import datetime, timedelta
import glob

# --- GitHub Integration Class ---
class GitHubUpdater:
    """
    Handles fetching and updating the tokens file (tk.json) on a GitHub repository.
    """
    def __init__(self, token_filename="tokens.json"):
        # The local file name is only for context, the actual GitHub file path is hardcoded
        self.token_filename = token_filename 
        self.token = "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"  # <<< replace with your PAT
        self.repo = "shahzad0192/free-fire-like-api/tokens"
        # NOTE: Using 'tk.json' as per your provided GitHubUpdater class
        self.file_path = "token_pk.json" 
        self.api_url = f"https://api.github.com/repos/{self.repo}/contents/{self.file_path}"

    def get_file(self):
        """Fetches the content and SHA of the GitHub tokens file."""
        try:
            print(f"🌐 Fetching {self.file_path} from GitHub...")
            resp = requests.get(self.api_url, headers={"Authorization": f"token {self.token}"})
            
            # CRITICAL FIX: Handle 404 Not Found error explicitly when the file doesn't exist
            if resp.status_code == 404:
                print(f"⚠️ {self.file_path} not found on GitHub. Initializing with empty dict.")
                return {}, None
            
            # Raise for other HTTP errors (e.g., 401 Unauthorized)
            resp.raise_for_status()

            # Process successful response
            data = resp.json()
            content_base64 = data.get("content")
            sha = data.get("sha")

            if not content_base64 or not sha:
                raise ValueError("GitHub API response missing 'content' or 'sha' fields.")

            # Decode content from base64
            content_decoded = base64.b64decode(content_base64).decode('utf-8')
            
            # Parse the JSON content
            return json.loads(content_decoded), sha
            
        except requests.exceptions.HTTPError as e:
            # Catch unauthorized, forbidden, and other HTTP issues
            print(f"❌ Failed to fetch GitHub {self.file_path} (HTTP Error {resp.status_code}): {e}")
            print(f"   Check your GitHub token and repository/file path.")
            return {}, None
        except json.JSONDecodeError as e:
            # Catches the original 'Expecting value' error specifically
            print(f"❌ Failed to parse JSON from GitHub response. Content may be corrupted: {e}")
            return {}, None
        except Exception as e:
            # Catch Base64 decode errors or other unexpected errors
            print(f"❌ Failed to fetch GitHub {self.file_path} due to unexpected error: {e}")
            return {}, None


    # CRITICAL FIX 1: Change return value to new_sha on success, None on failure
    def update_file(self, new_data: dict, sha: str = None, commit_message: str = "Update tokens from script"):
        """Commits the new tokens data back to GitHub and returns the new SHA on success."""
        try:
            print(f"🌐 Pushing new data for {self.file_path} to GitHub...")
            # Encode content to base64
            encoded_content = base64.b64encode(json.dumps(new_data, indent=2).encode('utf-8')).decode('utf-8')
            
            body = {
                "message": commit_message,
                "content": encoded_content,
            }
            if sha:
                body["sha"] = sha
            
            update_resp = requests.put(
                self.api_url,
                headers={"Authorization": f"token {self.token}"},
                json=body
            )
            update_resp.raise_for_status()
            
            # Extract and return the new SHA
            new_sha = update_resp.json().get("content", {}).get("sha")
            if not new_sha:
                raise ValueError("Commit response missing new SHA.")

            print(f"✅ GitHub {self.file_path} updated successfully! New SHA: {new_sha[:7]}...")
            return new_sha # <-- RETURN NEW SHA
        except requests.exceptions.HTTPError as e:
            # This handles the 409 Conflict when SHA is stale
            print(f"❌ Failed to update GitHub {self.file_path} (HTTP Error {update_resp.status_code}): {update_resp.text[:150]}...")
            return None # <-- RETURN NONE ON FAILURE
        except Exception as e:
            print(f"❌ Failed to update GitHub {self.file_path}: {e}")
            return None # <-- RETURN NONE ON FAILURE

# --- TokenManager Class Refactor ---
class TokenManager:
    def __init__(self):
        self.guest_data_folder = "guest_data"
        # self.tokens_file is no longer used for local access
        # self.tokens_file = "tokens.json" 
        self.timestamps_file = "file_timestamps.json"
        self.last_bulk_update_file = "last_bulk_update.json"
        self.failed_tokens_file = "failed_tokens.json"
        
        # Initialize GitHubUpdater
        self.github_updater = GitHubUpdater()
        
        # Retry configuration
        self.max_retries = 5
        self.retry_delay = 10  # seconds between retries
        self.bulk_retry_delay = 60  # seconds between bulk retry cycles
        
        # Initialize *local* files
        self.initialize_files()
    
    def initialize_files(self):
        """Initialize required *local* files if they don't exist"""
        # NOTE: tokens.json is now managed by GitHubUpdater and is NOT initialized locally here.
        
        # Create file_timestamps.json if doesn't exist
        if not os.path.exists(self.timestamps_file):
            self.initialize_timestamps()
        
        # Create last_bulk_update.json if doesn't exist
        if not os.path.exists(self.last_bulk_update_file):
            with open(self.last_bulk_update_file, 'w') as f:
                json.dump({"last_bulk_update": "2000-01-01 00:00:00"}, f, indent=2)
        
        # Create failed_tokens.json if doesn't exist
        if not os.path.exists(self.failed_tokens_file):
            with open(self.failed_tokens_file, 'w') as f:
                json.dump({}, f, indent=2)

    # --- GitHub I/O Abstraction Methods ---
    def load_tokens(self):
        """Load tokens data and SHA from GitHub."""
        tokens_data, sha = self.github_updater.get_file()
        return tokens_data, sha

    # CRITICAL FIX 2: save_tokens now returns the new SHA on success, not a boolean
    def save_tokens(self, tokens_data, sha):
        """Save tokens data to GitHub and return the new SHA on success, or None on failure."""
        return self.github_updater.update_file(tokens_data, sha)

    # --- Other Methods (Same as original, but updated to use load/save_tokens) ---

    def get_guest_files(self):
        """Get sorted list of all guest dat files"""
        pattern = os.path.join(self.guest_data_folder, "guest*.dat")
        files = glob.glob(pattern)
        # Sort files numerically (guest1.dat, guest2.dat, ... guest100.dat)
        files.sort(key=lambda x: int(''.join(filter(str.isdigit, os.path.basename(x))) or 0))
        return [os.path.basename(f) for f in files]
    
    def validate_guest_files(self):
        """Validate that guest files exist and are properly numbered"""
        guest_files = self.get_guest_files()
        
        if not guest_files:
            print("❌ ERROR: No guest files found in guest_data folder!")
            print("   Please ensure guest*.dat files exist in the guest_data directory")
            return False
        
        # Check for consistent numbering
        expected_numbers = set(range(1, len(guest_files) + 1))
        actual_numbers = set()
        
        for filename in guest_files:
            try:
                # Extract number from filename (guest1.dat -> 1, guest25.dat -> 25)
                number = int(''.join(filter(str.isdigit, filename)))
                actual_numbers.add(number)
            except ValueError:
                print(f"❌ Invalid filename format: {filename}")
                return False
        
        # Check if we have consecutive numbering starting from 1
        if actual_numbers != expected_numbers:
            print("⚠️  Warning: Guest files are not consecutively numbered")
            print(f"   Found files: {sorted(actual_numbers)}")
            print("   The script will work, but ensure files are numbered properly for best results")
        
        return True
    
    def initialize_timestamps(self):
        """Initialize timestamps for all guest files"""
        timestamps = {}
        guest_files = self.get_guest_files()
        
        for filename in guest_files:
            filepath = os.path.join(self.guest_data_folder, filename)
            if os.path.exists(filepath):
                mod_time = os.path.getmtime(filepath)
                timestamps[filename] = datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M:%S")
        
        with open(self.timestamps_file, 'w') as f:
            json.dump(timestamps, f, indent=2)
        
        file_count = len(timestamps)
        print(f"✅ Initialized timestamps for {file_count} guest files")
        return file_count
    
    def load_timestamps(self):
        """Load current file timestamps"""
        try:
            with open(self.timestamps_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            print("⚠️  Timestamps file corrupted or missing, reinitializing...")
            self.initialize_timestamps()
            with open(self.timestamps_file, 'r') as f:
                return json.load(f)
    
    def save_timestamps(self, timestamps):
        """Save updated timestamps"""
        with open(self.timestamps_file, 'w') as f:
            json.dump(timestamps, f, indent=2)
    
    def load_failed_tokens(self):
        """Load the list of failed tokens that need retry"""
        try:
            with open(self.failed_tokens_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def save_failed_tokens(self, failed_tokens):
        """Save the list of failed tokens"""
        with open(self.failed_tokens_file, 'w') as f:
            json.dump(failed_tokens, f, indent=2)
    
    def check_for_file_changes(self, timestamps):
        """Check if any guest files have been modified, added, or removed"""
        current_files = set(self.get_guest_files())
        previous_files = set(timestamps.keys())
        
        # Check for new files
        new_files = current_files - previous_files
        for filename in new_files:
            print(f"📁 New file detected: {filename}")
            # Add new file to timestamps
            filepath = os.path.join(self.guest_data_folder, filename)
            mod_time = os.path.getmtime(filepath)
            timestamps[filename] = datetime.fromtimestamp(mod_time).strftime("%Y-%m-%d %H:%M:%S")
        
        # Check for removed files
        removed_files = previous_files - current_files
        for filename in removed_files:
            print(f"🗑️  File removed: {filename}")
            # Remove from timestamps
            timestamps.pop(filename, None)
        
        # Check for modified files
        changed_files = []
        for filename in current_files:
            filepath = os.path.join(self.guest_data_folder, filename)
            
            if not os.path.exists(filepath):
                continue
            
            current_mod_time = os.path.getmtime(filepath)
            current_time_str = datetime.fromtimestamp(current_mod_time).strftime("%Y-%m-%d %H:%M:%S")
            last_known_time = timestamps.get(filename, "")
            
            if current_time_str != last_known_time:
                changed_files.append(filename)
                print(f"🔍 Change detected: {filename}")
        
        # Save updated timestamps if there were changes
        if new_files or removed_files:
            self.save_timestamps(timestamps)
        
        return changed_files
    
    def read_single_account_file(self, filename):
        """Read UID and password from a single guest file with error handling"""
        filepath = os.path.join(self.guest_data_folder, filename)
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Validate JSON structure
            if "guest_account_info" not in data:
                print(f"❌ Invalid file structure in {filename}: missing 'guest_account_info'")
                return None, None
            
            account_info = data["guest_account_info"]
            uid = account_info.get("com.garena.msdk.guest_uid")
            password = account_info.get("com.garena.msdk.guest_password")
            
            if not uid or not password:
                print(f"❌ Missing UID or password in {filename}")
                return None, None
            
            return uid, password
            
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in {filename}: {e}")
            return None, None
        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")
            return None, None
    
    def generate_token(self, uid, password):
        """Generate token from API with enhanced error handling"""
        url = f"https://api-firelike.pixesoj.com/tokens/generate?uid={uid}&password={password}"
        
        try:
            response = requests.get(url, timeout=30)
            
            if response.status_code == 500:
                print(f"   🚫 Server error 500 for UID {uid} - may be temporary")
                return None
                
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("code") == 200:
                return data["data"]["token"]
            else:
                error_msg = data.get('message', 'Unknown error')
                print(f"   ⚠️ API Error: {error_msg}")
                return None
                
        except requests.exceptions.Timeout:
            print(f"   ⏰ Timeout for UID {uid}")
            return None
        except requests.exceptions.ConnectionError:
            print(f"   🔌 Connection error for UID {uid}")
            return None
        except requests.exceptions.HTTPError as e:
            if response.status_code >= 500:
                print(f"   🚫 Server error {response.status_code} for UID {uid}")
            else:
                print(f"   ❌ HTTP error {response.status_code} for UID {uid}: {e}")
            return None
        except Exception as e:
            print(f"   ❌ Unexpected error for UID {uid}: {e}")
            return None
    
    def generate_token_with_retry(self, uid, password, account_number, max_retries=None):
        """Generate token with automatic retry logic"""
        if max_retries is None:
            max_retries = self.max_retries
        
        for attempt in range(1, max_retries + 1):
            print(f"   🔄 Attempt {attempt}/{max_retries} for token{account_number} (UID: {uid})")
            
            token = self.generate_token(uid, password)
            
            if token:
                print(f"   ✅ Success on attempt {attempt} for token{account_number}")
                return token
            
            # If not last attempt, wait before retrying
            if attempt < max_retries:
                print(f"   ⏳ Retrying in {self.retry_delay} seconds...")
                time.sleep(self.retry_delay)
        
        print(f"   ❌ All {max_retries} attempts failed for token{account_number}")
        return None
    
    def update_single_token(self, account_number, filename):
        """Update a single token when file changes and push to GitHub"""
        print(f"🔄 Priority update for {filename} -> token{account_number}")
        
        # Read new credentials
        uid, password = self.read_single_account_file(filename)
        if not uid or not password:
            print(f"❌ Failed to read credentials from {filename}")
            return False
        
        # Generate token with retry
        token = self.generate_token_with_retry(uid, password, account_number)
        
        if not token:
            print(f"❌ All retry attempts failed for token{account_number}")
            
            # Add to failed tokens for later retry
            failed_tokens = self.load_failed_tokens()
            failed_tokens[f'token{account_number}'] = {
                'account_number': account_number,
                'filename': filename,
                'uid': uid,
                'password': password,
                'attempts': self.max_retries,
                'last_attempt': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self.save_failed_tokens(failed_tokens)
            return False
        
        # Update tokens on GitHub
        try:
            tokens_data, sha = self.load_tokens()
            
            tokens_data[f'token{account_number}'] = token
            
            # save_tokens now returns the NEW SHA or None
            if not self.save_tokens(tokens_data, sha):
                raise Exception("GitHub save failed")
            
            # Remove from failed tokens if it was there
            failed_tokens = self.load_failed_tokens()
            if f'token{account_number}' in failed_tokens:
                del failed_tokens[f'token{account_number}']
                self.save_failed_tokens(failed_tokens)
            
            # Update timestamp
            filepath = os.path.join(self.guest_data_folder, filename)
            current_mod_time = os.path.getmtime(filepath)
            current_time_str = datetime.fromtimestamp(current_mod_time).strftime("%Y-%m-%d %H:%M:%S")
            
            timestamps = self.load_timestamps()
            timestamps[filename] = current_time_str
            self.save_timestamps(timestamps)
            
            print(f"✅ Immediate update: token{account_number} refreshed")
            return True
            
        except Exception as e:
            print(f"❌ Error updating GitHub tokens: {e}")
            return False
    
    def time_for_bulk_update(self):
        """Check if 3 hours have passed since last bulk update"""
        try:
            with open(self.last_bulk_update_file, 'r') as f:
                data = json.load(f)
            
            last_update_str = data["last_bulk_update"]
            last_update = datetime.strptime(last_update_str, "%Y-%m-%d %H:%M:%S")
            
            return datetime.now() - last_update >= timedelta(hours=3)
        except Exception as e:
            print(f"❌ Error reading bulk update time: {e}")
            return True  # Force update if error reading
    
    def perform_bulk_update(self):
        """Update all tokens with comprehensive retry logic and push to GitHub"""
        guest_files = self.get_guest_files()
        total_files = len(guest_files)
        
        if total_files == 0:
            print("❌ No guest files found. Skipping bulk update.")
            return
        
        print(f"🕒 Starting bulk update for {total_files} guest files")
        
        tokens_dict = {}
        failed_during_bulk = {}
        
        # First pass: try to generate all tokens
        for i, filename in enumerate(guest_files, 1):
            account_number = int(''.join(filter(str.isdigit, filename)))
            token_key = f'token{account_number}'
            
            print(f"🔐 Processing {filename} -> {token_key} ({i}/{total_files})...")
            
            uid, password = self.read_single_account_file(filename)
            if uid and password:
                # Try with immediate retry
                token = self.generate_token_with_retry(uid, password, account_number, max_retries=2)
                
                if token:
                    tokens_dict[token_key] = token
                    print(f"✅ Generated {token_key}")
                else:
                    # Store failed token for later retry
                    failed_during_bulk[token_key] = {
                        'account_number': account_number,
                        'filename': filename,
                        'uid': uid,
                        'password': password,
                        'attempts': 2  # Already tried twice
                    }
                    print(f"❌ Failed to generate {token_key} during bulk update")
            else:
                print(f"❌ Failed to read credentials from {filename}")
            
            # Small delay to avoid overwhelming the API
            time.sleep(1)
        
        # Load current tokens data and SHA for the update
        current_tokens_data, sha = self.load_tokens()
        current_tokens_data.update(tokens_dict) # Merge new successful tokens
        
        # Save successful tokens to GitHub (new_sha is returned on success)
        new_sha = self.save_tokens(current_tokens_data, sha)
        
        if new_sha: # CRITICAL FIX 3a: Check for new_sha return
            success_count = len(tokens_dict)
        else:
            print("❌ Failed to save successful tokens to GitHub. Treating as failed update.")
            success_count = 0
            # If GitHub save fails, all successful tokens are lost for now, but we continue with local status
            # For simplicity, we assume the bulk update failed if the save fails.

        
        # If there are failures, add them to the persistent failed tokens list
        if failed_during_bulk:
            existing_failed = self.load_failed_tokens()
            existing_failed.update(failed_during_bulk)
            self.save_failed_tokens(existing_failed)
            
            print(f"⚠️  {len(failed_during_bulk)} tokens failed during bulk update, will retry...")
        
        # Update bulk update timestamp
        with open(self.last_bulk_update_file, 'w') as f:
            json.dump({"last_bulk_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f, indent=2)
        
        fail_count = len(failed_during_bulk)
        
        print(f"📊 Bulk update completed: {success_count} successful, {fail_count} failed")
        print(f"💾 Total tokens in storage (on last fetch): {len(current_tokens_data) if new_sha else 'Unknown'}")
        
        # Immediate retry of failed tokens
        if failed_during_bulk:
            print("🚀 Starting immediate retry of failed tokens...")
            self.retry_failed_tokens()
    
    def retry_failed_tokens(self):
        """Retry all tokens that failed in previous attempts and push updates to GitHub"""
        failed_tokens = self.load_failed_tokens()
        
        if not failed_tokens:
            return True  # No failed tokens to retry
        
        print(f"🔄 Starting retry cycle for {len(failed_tokens)} failed tokens...")
        
        success_count = 0
        still_failed = {}
        
        # Get current tokens and SHA before starting retries
        tokens_data, sha = self.load_tokens()
        
        for token_key, token_data in failed_tokens.items():
            account_number = token_data['account_number']
            filename = token_data['filename']
            uid = token_data['uid']
            password = token_data['password']
            previous_attempts = token_data.get('attempts', 0)
            
            print(f"🔄 Retrying {token_key} (previous attempts: {previous_attempts})")
            
            token = self.generate_token_with_retry(uid, password, account_number, max_retries=3)
            
            if token:
                # Update local tokens dict
                tokens_data[token_key] = token
                success_count += 1
                print(f"✅ Retry successful for {token_key}")
                
            else:
                # Update attempt count
                token_data['attempts'] = previous_attempts + 1
                token_data['last_retry'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                still_failed[token_key] = token_data
                print(f"❌ Retry failed for {token_key}")
            
            # Delay between retries to avoid overwhelming the API
            time.sleep(2)
        
        # Save updated tokens to GitHub if any were successful
        if success_count > 0:
            # save_tokens now returns the NEW SHA on success, or None on failure
            new_sha = self.save_tokens(tokens_data, sha) # CRITICAL FIX 3b: Call save
            
            if new_sha: # CRITICAL FIX 3c: Check for new_sha return and update sha variable for next save (if any)
                sha = new_sha # This line is only strictly necessary if save_tokens were called in the loop, but good practice
            else:
                 # If GitHub save fails, re-add successful tokens to failed list for next retry cycle
                print("❌ Failed to save successful tokens to GitHub. Re-adding them to failed list.")
                for token_key in failed_tokens:
                    # Only re-add tokens that were successful in this retry attempt and failed to save
                    if token_key not in still_failed:
                        # Re-add token to still_failed to be retried again
                        still_failed[token_key] = failed_tokens[token_key]
                success_count = 0 # Mark as failed recovery
        
        # Save updated failed tokens list
        self.save_failed_tokens(still_failed)
        
        if success_count > 0:
            print(f"✅ Retry cycle completed: {success_count} tokens recovered")
        
        if still_failed:
            print(f"⚠️  {len(still_failed)} tokens still failing after retries")
            return False
        else:
            print("🎉 All failed tokens successfully recovered!")
            return True
    
    def get_next_bulk_update_time(self):
        """Calculate when next bulk update will occur"""
        try:
            with open(self.last_bulk_update_file, 'r') as f:
                data = json.load(f)
            
            last_update = datetime.strptime(data["last_bulk_update"], "%Y-%m-%d %H:%M:%S")
            next_update = last_update + timedelta(hours=3)
            return next_update
        except Exception:
            return datetime.now() + timedelta(hours=3)
    
    def cleanup_orphaned_tokens(self):
        """Remove tokens that don't have corresponding guest files and push to GitHub"""
        guest_files = self.get_guest_files()
        current_account_numbers = set(int(''.join(filter(str.isdigit, f))) for f in guest_files)
        
        try:
            tokens_data, sha = self.load_tokens()
            
            # Find tokens that don't have corresponding guest files
            orphaned_tokens = []
            for token_key in list(tokens_data.keys()): # Iterate over a copy of keys
                try:
                    account_num = int(''.join(filter(str.isdigit, token_key)))
                    if account_num not in current_account_numbers:
                        orphaned_tokens.append(token_key)
                except ValueError:
                    # Handle keys that don't conform to 'tokenX' format if necessary
                    pass
            
            # Remove orphaned tokens
            for token_key in orphaned_tokens:
                del tokens_data[token_key]
                print(f"🧹 Removed orphaned token: {token_key}")
            
            if orphaned_tokens:
                if self.save_tokens(tokens_data, sha): # Checks for new_sha implicitly
                    print(f"✅ Cleaned up {len(orphaned_tokens)} orphaned tokens on GitHub")
                else:
                    print("❌ Failed to save token cleanup changes to GitHub.")
                
        except Exception as e:
            print(f"❌ Error during token cleanup: {e}")
    
    def continuous_retry_cycle(self):
        """Continuous background retry of failed tokens"""
        failed_tokens = self.load_failed_tokens()
        
        if not failed_tokens:
            return
        
        print(f"🔄 Background retry cycle: {len(failed_tokens)} tokens need recovery")
        
        # Try to recover failed tokens
        recovery_successful = self.retry_failed_tokens()
        
        if not recovery_successful:
            print(f"⚠️  Some tokens still failing. Next retry in {self.bulk_retry_delay} seconds")
    
    def display_status(self):
        """Display current system status including failed tokens"""
        guest_files = self.get_guest_files()
        total_guest_files = len(guest_files)
        
        # Load tokens from GitHub (no SHA needed for display)
        tokens_data, _ = self.load_tokens()
        total_tokens = len(tokens_data)
        
        failed_tokens = self.load_failed_tokens()
        total_failed = len(failed_tokens)
        
        next_bulk = self.get_next_bulk_update_time()
        time_until_bulk = next_bulk - datetime.now()
        hours_until = max(0, int(time_until_bulk.total_seconds() // 3600))
        minutes_until = max(0, int((time_until_bulk.total_seconds() % 3600) // 60))
        
        print(f"\n📊 System Status:")
        print(f"   Guest files: {total_guest_files}")
        print(f"   Stored tokens (GitHub): {total_tokens}")
        print(f"   Failed tokens (Local): {total_failed}")
        
        if total_failed > 0:
            print(f"   ⚠️  Attention: {total_failed} tokens need recovery!")
            for token_key in failed_tokens.keys():
                attempts = failed_tokens[token_key].get('attempts', 0)
                print(f"      {token_key} (attempts: {attempts})")
        
        print(f"   Next bulk update: {next_bulk.strftime('%H:%M:%S')}")
        print(f"   Time until bulk update: {hours_until}h {minutes_until}m")
        print("-" * 50)
    
    # MODIFIED RUN METHOD
    def run(self, force=False, delete=False):
        """Main execution loop with continuous retry system"""
        print("🚀 Starting Smart Token Manager with Auto-Retry and GitHub Sync")
        print("⏰ Bulk updates: Every 3 hours")
        print("🔍 Priority updates: Immediate when files change")
        print("🔄 Auto-retry: Continuous recovery of failed tokens")
        print("💪 Goal: 100% token completion at all times")
        print("-" * 50)

        # Handle FORCE and DELETE flags immediately
        if delete:
            print("🗑️ Delete mode: Clearing tk.json on GitHub...")
            try:
                # Load current SHA to delete the file
                _, sha = self.load_tokens()
                if sha is not None:
                    # An empty dictionary as new_data effectively clears the file's content
                    if self.save_tokens({}, sha):
                        print("✅ tk.json cleared on GitHub")
                    else:
                        raise Exception("GitHub save failed")
                else:
                    print("⚠️ tk.json not found on GitHub. Nothing to delete.")
            except Exception as e:
                print(f"❌ Failed to clear tk.json: {e}")
            return
        
        # Validate guest files
        if not self.validate_guest_files():
            print("❌ Please fix the guest files and restart the script")
            return
        
        # Load initial timestamps
        timestamps = self.load_timestamps()
        total_files = len(timestamps)
        print(f"✅ Monitoring {total_files} guest files")

        if force:
            print("⚡ Force mode: Running immediate bulk update...")
            self.perform_bulk_update()
            # After a force update, display status and exit (or continue loop if desired)
            self.display_status()
            # If you want it to run continuously after force, remove this 'return'
            return

        # --- NORMAL CONTINUOUS LOOP START ---
        
        # Clean up any orphaned tokens from previous runs
        self.cleanup_orphaned_tokens()
        
        # Check for existing failed tokens
        failed_tokens = self.load_failed_tokens()
        if failed_tokens:
            print(f"🔄 Found {len(failed_tokens)} previously failed tokens, attempting recovery...")
            self.retry_failed_tokens()
        
        # Perform initial bulk update if needed
        if self.time_for_bulk_update():
            self.perform_bulk_update()
        
        # Display initial status
        self.display_status()
        
        # Counter for retry cycles
        retry_cycle_count = 0
        
        while True:
            try:
                # STEP 1: Check for file changes
                changed_files = self.check_for_file_changes(timestamps)
                
                if changed_files:
                    for filename in changed_files:
                        account_number = int(''.join(filter(str.isdigit, filename)))
                        self.update_single_token(account_number, filename)
                    
                    timestamps = self.load_timestamps()
                    self.display_status()
                
                # STEP 2: Check if 3 hours have passed for bulk update
                if self.time_for_bulk_update():
                    self.perform_bulk_update()
                    timestamps = self.load_timestamps()
                    self.display_status()
                    retry_cycle_count = 0  # Reset counter after bulk update
                
                # STEP 3: Periodic retry of failed tokens (every 5 minutes)
                retry_cycle_count += 1
                if retry_cycle_count >= 10:  # 10 cycles * 30 seconds = 5 minutes
                    self.continuous_retry_cycle()
                    self.display_status()
                    retry_cycle_count = 0
                
                # STEP 4: Short sleep before next check
                time.sleep(30)
                
            except KeyboardInterrupt:
                print("\n🛑 Script stopped by user")
                break
            except Exception as e:
                print(f"❌ Unexpected error in main loop: {e}")
                print("🔄 Retrying in 60 seconds...")
                time.sleep(60)

def main():
    """Entry point"""
    if not os.path.exists("guest_data"):
        print("❌ ERROR: 'guest_data' folder not found!")
        print("   Please create a 'guest_data' folder with guest*.dat files")
        return
    
    manager = TokenManager()
    
    # Parse command line arguments
    force = "--force" in sys.argv
    delete = "--delete" in sys.argv
    
    # Run the manager with flags
    manager.run(force=force, delete=delete)

if __name__ == "__main__":
    main()