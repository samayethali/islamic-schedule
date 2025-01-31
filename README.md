# Islamic Schedule

1. Set up a Google Cloud Project
   - Go to https://console.cloud.google.com/
   - Click on "Select a Project" at the top of the page
   - Click "New Project"
   - Enter a project name (e.g., "Calendar Scheduler")
   - Click "Create"
   - Wait for the project to be created and select it as your current project

2. Enable the Google Calendar API
   - In the Google Cloud Console, go to the Navigation Menu (☰)
   - Select "APIs & Services" > "Library"
   - Search for "Google Calendar API"
   - Click on "Google Calendar API"
   - Click "Enable"

3. Create credentials (service account or OAuth 2.0)
   - In the Google Cloud Console, go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" at the top of the page
   - Select "OAuth client ID"

4. If prompted, configure the OAuth consent screen:
   - Click "Configure Consent Screen"
   - Choose "External" user type
   - Fill in the required fields (App name, user support email, developer contact email)
   - Click "Save and Continue"
   - Add the "Google Calendar API ./auth/calendar" scope
   - Add your email as a test user
   - Click "Save and Continue"
   - Review and go back to credentials
e. Create OAuth client ID:
   - Select "Desktop app" as the application type
   - Give it a name (e.g., "Calendar Scheduler")
   - Click "Create"
   - Download the client configuration file (it will be a JSON file) and save it as credentials.json in the directory of this git repo

5. Install required packages:
   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client
   ```

6. Replace 'Europe/London' with your timezone (e.g., 'America/New_York') in `main.py` if needed

7. Run `python3 main.py`
