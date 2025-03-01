import argparse
import csv
import datetime
import logging
import os
import sys
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from google_auth_oauthlib.flow import InstalledAppFlow
from zoneinfo import ZoneInfo

# Constants
SCOPES = ['https://www.googleapis.com/auth/calendar']
TIME_ZONE = ZoneInfo('Europe/London')
DEFAULT_LOG_LEVEL = logging.INFO
PRAYER_TIMES_DIR = 'prayer-times'
CSV_DATE_FORMAT = '%d %b'  # e.g. '01 Mar'
MAX_DATE_INPUT_ATTEMPTS = 5
CSV_FILENAME_FORMAT = '%b-%Y.csv'  # e.g. 'apr-2025.csv'
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'
LOG_FILE = 'calendar_sync.log'


# Column Names
class CSVColumn(Enum):
    DATE = 'Date'
    FAJR_BEGINS = 'Fajr Begins'
    FAJR_JAMAAH = 'Fajr Jama\'ah'
    SUNRISE = 'Sunrise'
    DHUHR_BEGINS = 'Dhuhr Begins'
    DHUHR_JAMAAH = 'Dhuhr Jama\'ah'
    ASR_BEGINS = 'Asr Begins'
    ASR_JAMAAH = 'Asr Jama\'ah'
    MAGHRIB = 'Maghrib'
    ISHA_BEGINS = 'Isha\'a Begins'
    ISHA_JAMAAH = 'Isha\'a Jama\'ah'


# Event Colors
class Color(Enum):
    LAVENDER = '1'


# Event Configuration
@dataclass(frozen=True)
class EventConfig:
    summary: str
    csv_column: CSVColumn
    start_adjust: int
    end_adjust: int


# These configuration values are still used for offsetting.
# (They are now used only for non-Tahajjud/Fajr events.)
EVENT_CONFIGS = [
    EventConfig('Suḥūr', CSVColumn.FAJR_BEGINS, -40, 0),
    EventConfig('Fajr & Morning Adhkār', CSVColumn.FAJR_BEGINS, 0, 45),
    EventConfig('Ẓuhr & News', CSVColumn.DHUHR_JAMAAH, -30, 60),
    EventConfig("'Aṣr & Evening Adhkār", CSVColumn.ASR_JAMAAH, -60, -15),
    EventConfig('Maghrib', CSVColumn.MAGHRIB, -10, 80),
    EventConfig("'Ishā", CSVColumn.ISHA_JAMAAH, -15, 30),
]


def configure_logging() -> None:
    """Configures logging with separate file and console handlers."""
    logger = logging.getLogger()
    logger.setLevel(DEFAULT_LOG_LEVEL)

    # Remove existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler for detailed logs
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console handler for general info
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def check_prayer_times_directory() -> None:
    """Validates the existence of the prayer times directory."""
    if not os.path.isdir(PRAYER_TIMES_DIR):
        logging.critical(f"Missing directory: {PRAYER_TIMES_DIR}")
        sys.exit(1)


def parse_time(time_str: str) -> Optional[datetime.time]:
    """Parses a time string into a datetime.time object."""
    try:
        # Try parsing as 24-hour format first
        return datetime.datetime.strptime(time_str, '%H:%M').time()
    except ValueError:
        try:
            # If that fails, try 12-hour format
            time_parts = time_str.split(':')
            if len(time_parts) == 2:
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                
                # Special handling for prayer times
                # For Isha times (typically evening), 8:30 should be PM (20:30)
                if hour == 8 or hour == 9:
                    hour += 12
                # For times 1-6, assume PM (13:00-18:00)
                elif hour < 7 and hour != 12:
                    hour += 12
                
                return datetime.time(hour, minute)
        except (ValueError, IndexError) as e:
            logging.error(f"Invalid time format '{time_str}': {e}")
            return None
    return None


def round_time(time_obj: datetime.time, round_to: int = 5) -> datetime.time:
    """Rounds a time object to the nearest specified minute interval."""
    total_minutes = time_obj.hour * 60 + time_obj.minute
    rounded_total = round(total_minutes / round_to) * round_to
    rounded_hour = (rounded_total // 60) % 24
    rounded_minute = rounded_total % 60
    return datetime.time(rounded_hour, rounded_minute)


def adjust_time_custom(time_str: str, adjustment: int, round_to: int) -> Optional[datetime.time]:
    """Adjusts a time string by a specific number of minutes and rounds it to the specified interval."""
    # Use the parse_time function which now handles all special cases
    base_time = parse_time(time_str)
    
    if not base_time:
        return None
    
    base_dt = datetime.datetime.combine(datetime.date.today(), base_time)
    adjusted_dt = base_dt + datetime.timedelta(minutes=adjustment)
    return round_time(adjusted_dt.time(), round_to)


class GoogleCalendarService:
    """Handles authentication and interactions with Google Calendar API."""

    def __init__(self) -> None:
        self.credentials: Optional[Credentials] = self.load_credentials()
        if not self.credentials or not self.credentials.valid:
            self.credentials = self.authenticate()
            self.save_credentials()
        self.service: Resource = self.build_service()

    def load_credentials(self) -> Optional[Credentials]:
        """Loads credentials from the token file."""
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
            logging.debug("Credentials loaded successfully.")
            return creds
        except (ValueError, FileNotFoundError) as e:
            logging.debug(f"Failed to load credentials: {e}")
            return None

    def authenticate(self) -> Credentials:
        """Performs OAuth2 authentication flow."""
        if os.path.exists(CREDENTIALS_PATH):
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
            logging.info("Authentication successful.")
            return creds
        else:
            logging.critical(f"Missing {CREDENTIALS_PATH} file.")
            sys.exit(1)

    def save_credentials(self) -> None:
        """Saves credentials to the token file."""
        try:
            with open(TOKEN_PATH, 'w', encoding='utf-8') as token_file:
                token_file.write(self.credentials.to_json())
            logging.debug("Credentials saved successfully.")
        except IOError as e:
            logging.error(f"Failed to save credentials: {e}")

    def build_service(self) -> Resource:
        """Builds the Google Calendar service."""
        try:
            service = build('calendar', 'v3', credentials=self.credentials, cache_discovery=False)
            logging.debug("Google Calendar service built successfully.")
            return service
        except Exception as e:
            logging.critical(f"Failed to build Google Calendar service: {e}")
            sys.exit(1)

    def refresh_credentials(self) -> None:
        """Refreshes expired credentials."""
        if self.credentials and self.credentials.expired and self.credentials.refresh_token:
            try:
                self.credentials.refresh(Request())
                self.save_credentials()
                logging.info("Credentials refreshed successfully.")
            except Exception as e:
                logging.error(f"Failed to refresh credentials: {e}")
                self.credentials = self.authenticate()
                self.save_credentials()


def load_csv_data(filename: str) -> Dict[datetime.date, Dict[str, str]]:
    """Loads CSV data into a dictionary mapped by date."""
    data: Dict[datetime.date, Dict[str, str]] = {}
    filepath = os.path.join(PRAYER_TIMES_DIR, filename)
    
    # Extract year from filename (e.g., 'mar-2025.csv' -> '2025')
    try:
        year_str = filename.split('-')[1].split('.')[0]
        year = int(year_str)
    except (IndexError, ValueError):
        logging.error(f"Could not extract year from filename: {filename}")
        return data
    
    try:
        with open(filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                date_str = row.get(CSVColumn.DATE.value, '').strip()
                try:
                    # Parse date with day and month only, then add the year
                    date_obj = datetime.datetime.strptime(date_str, CSV_DATE_FORMAT)
                    row_date = date_obj.replace(year=year).date()
                    data[row_date] = row
                except ValueError:
                    logging.warning(f"Invalid date format in row: '{date_str}'")
        logging.debug(f"Loaded data from {filename} with {len(data)} entries.")
    except FileNotFoundError:
        logging.warning(f"Missing CSV file: {filepath}")
    return data


def create_event(
    service: Optional[Resource],
    summary: str,
    start_time: datetime.time,
    end_time: datetime.time,
    event_date: datetime.date,
    color: Color,
    dry_run: bool = False
) -> bool:
    """Creates a Google Calendar event."""
    start_dt = datetime.datetime.combine(event_date, start_time, tzinfo=TIME_ZONE)
    end_dt = datetime.datetime.combine(event_date, end_time, tzinfo=TIME_ZONE)
    if end_dt <= start_dt:
        end_dt += datetime.timedelta(days=1)
    
    if dry_run:
        logging.info(f"[DRY RUN] Would create event: '{summary}' on {event_date.strftime('%d %b %Y')} from {start_time} to {end_time}")
        return True
    
    try:
        event_body = {
            'summary': summary,
            'start': {'dateTime': start_dt.isoformat(), 'timeZone': TIME_ZONE.key},
            'end': {'dateTime': end_dt.isoformat(), 'timeZone': TIME_ZONE.key},
            'colorId': color.value
        }
        service.events().insert(calendarId='primary', body=event_body).execute()
        logging.info(f"Created event: '{summary}' on {event_date.strftime('%d %b %Y')}")
        return True
    except Exception as e:
        logging.error(f"Failed to create event '{summary}' on {event_date}: {e}")
        return False


def calculate_isha_end(
    maghrib_time: datetime.time,
    fajr_time: datetime.time,
    event_date: datetime.date,
    start_date: datetime.date,
    end_date: datetime.date
) -> Optional[Tuple[datetime.time, datetime.time, datetime.date]]:
    """Calculates Ishā End time.
    
    The start time is exactly the midpoint between Maghrib (of one day)
    and Fajr (of the next day) with no rounding, and the end time is 15 minutes later.
    """
    maghrib_dt = datetime.datetime.combine(event_date, maghrib_time, tzinfo=TIME_ZONE)
    next_day = event_date + datetime.timedelta(days=1)
    fajr_dt = datetime.datetime.combine(next_day, fajr_time, tzinfo=TIME_ZONE)
    if maghrib_dt >= fajr_dt:
        logging.warning("Maghrib time is not before Fajr time for Ishā calculation.")
        return None
    midpoint = maghrib_dt + (fajr_dt - maghrib_dt) / 2
    isha_end = midpoint + datetime.timedelta(minutes=15)
    target_date = midpoint.date()
    if not (start_date <= target_date <= end_date):
        logging.debug(f"Ishā End date {target_date} is out of range.")
        return None
    # Do not round the midpoint; use it exactly.
    return (
        midpoint.time(),
        isha_end.time(),
        target_date
    )


def convert_to_24h_format(time_str: str, prayer_type: str) -> Optional[datetime.time]:
    """Converts a time string to 24-hour format based on prayer type."""
    try:
        # Parse the time string into hours and minutes
        time_parts = time_str.split(':')
        if len(time_parts) != 2:
            raise ValueError(f"Invalid time format: {time_str}")
        
        hour = int(time_parts[0])
        minute = int(time_parts[1])
        
        # Apply explicit conversion rules based on prayer type and hour
        if prayer_type == 'fajr':
            # Fajr is early morning (typically 4-6 AM)
            # No conversion needed as it's already in 24-hour format
            pass
        elif prayer_type == 'dhuhr':
            # Dhuhr is around noon/early afternoon
            # Convert 1:15 to 13:15
            if hour < 12:
                hour += 12
        elif prayer_type == 'asr':
            # Asr is in the afternoon (typically 3-5 PM)
            # Convert 3:49 to 15:49, 4:15 to 16:15
            if hour < 12:
                hour += 12
        elif prayer_type == 'maghrib':
            # Maghrib is in the evening (typically 5-7 PM)
            # Convert 5:45 to 17:45
            if hour < 12:
                hour += 12
        elif prayer_type == 'isha':
            # Isha is at night (typically 8-10 PM)
            # Convert 8:30 to 20:30
            if hour < 12:
                hour += 12
        
        return datetime.time(hour, minute)
    except (ValueError, IndexError) as e:
        logging.error(f"Invalid time format '{time_str}' for {prayer_type}: {e}")
        return None

def process_prayer_events(
    service: Optional[Resource],
    row: Dict[str, str],
    event_date: datetime.date,
    start_date: datetime.date,
    end_date: datetime.date,
    next_day_row: Optional[Dict[str, str]],
    dry_run: bool = False
) -> None:
    """Processes and creates prayer time events for a specific date."""
    # Process each event from the configuration
    for config in EVENT_CONFIGS:
        time_str = row.get(config.csv_column.value, '').strip()
        if not time_str:
            logging.debug(f"No time data for '{config.summary}' on {event_date}")
            continue

        # Determine prayer type for time conversion
        prayer_type = 'fajr'
        if 'Ẓuhr' in config.summary or 'Dhuhr' in config.summary:
            prayer_type = 'dhuhr'
        elif 'Aṣr' in config.summary or 'Asr' in config.summary:
            prayer_type = 'asr'
        elif 'Maghrib' in config.summary:
            prayer_type = 'maghrib'
        elif 'Ishā' in config.summary or 'Isha' in config.summary:
            prayer_type = 'isha'

        # Special handling for Suḥūr
        if config.summary == 'Suḥūr':
            base_time = convert_to_24h_format(time_str, 'fajr')
            if not base_time:
                continue
            base_dt = datetime.datetime.combine(event_date, base_time)
            # Start: 40 minutes before Fajr Begins, rounded to nearest 5 minutes.
            start_time = round_time((base_dt + datetime.timedelta(minutes=-40)).time(), 5)
            # End: exactly Fajr Begins (no rounding)
            end_time = base_time

        # Special handling for Fajr
        elif config.summary.startswith('Fajr'):
            base_time = convert_to_24h_format(time_str, 'fajr')
            if not base_time:
                continue
            # Start: exactly Fajr Begins (no rounding)
            start_time = base_time
            # End: 45 minutes after Fajr Begins, rounded to the nearest 15 minutes.
            end_dt = datetime.datetime.combine(event_date, base_time) + datetime.timedelta(minutes=45)
            end_time = round_time(end_dt.time(), 15)

        # Special handling for Dhuhr in March 2025
        elif 'Ẓuhr' in config.summary or 'Dhuhr' in config.summary:
            # Check if the date is in March 2025
            if event_date.month == 3 and event_date.year == 2025:
                # Check if the day is Friday (weekday 4 in Python's datetime)
                if event_date.weekday() == 4:  # Friday
                    # Start at 12pm and last for 3 hours
                    start_time = datetime.time(12, 0)
                    end_time = datetime.time(15, 0)
                else:
                    # Start at 1pm and last for 2 hours
                    start_time = datetime.time(13, 0)
                    end_time = datetime.time(15, 0)
            else:
                # Use normal processing for other months
                base_time = convert_to_24h_format(time_str, prayer_type)
                if not base_time:
                    continue
                base_dt = datetime.datetime.combine(event_date, base_time)
                start_dt = base_dt + datetime.timedelta(minutes=config.start_adjust)
                end_dt = base_dt + datetime.timedelta(minutes=config.end_adjust)
                start_time = round_time(start_dt.time(), 15)
                end_time = round_time(end_dt.time(), 15)
        # All other events: use the configured offsets and round to the nearest 15 minutes.
        else:
            base_time = convert_to_24h_format(time_str, prayer_type)
            if not base_time:
                continue
            base_dt = datetime.datetime.combine(event_date, base_time)
            start_dt = base_dt + datetime.timedelta(minutes=config.start_adjust)
            end_dt = base_dt + datetime.timedelta(minutes=config.end_adjust)
            start_time = round_time(start_dt.time(), 15)
            end_time = round_time(end_dt.time(), 15)

        if not start_time or not end_time:
            logging.debug(f"Skipping event '{config.summary}' due to invalid time for {event_date}")
            continue

        if not (start_date <= event_date <= end_date):
            logging.debug(f"Date {event_date} is outside the processing range.")
            continue

        create_event(
            service,
            config.summary,
            start_time,
            end_time,
            event_date,
            Color.LAVENDER,
            dry_run
        )

    # Handle Tarāwīḥ event (1 hour duration, ending at Suḥūr time)
    fajr_str = row.get(CSVColumn.FAJR_BEGINS.value, '').strip()
    if fajr_str:
        fajr_time = convert_to_24h_format(fajr_str, 'fajr')
        if fajr_time:
            # Calculate Suḥūr start time (40 minutes before Fajr)
            fajr_dt = datetime.datetime.combine(event_date, fajr_time)
            suhur_start_dt = fajr_dt + datetime.timedelta(minutes=-40)
            suhur_start_time = round_time(suhur_start_dt.time(), 5)
            
            # Calculate Tarāwīḥ start time (1 hour before Suḥūr starts)
            tarawih_start_dt = suhur_start_dt + datetime.timedelta(minutes=-60)
            tarawih_start_time = round_time(tarawih_start_dt.time(), 5)
            
            # Tarāwīḥ ends when Suḥūr starts
            tarawih_end_time = suhur_start_time
            
            # Create Tarāwīḥ event
            create_event(
                service,
                'Tarāwīḥ',
                tarawih_start_time,
                tarawih_end_time,
                event_date,
                Color.LAVENDER,
                dry_run
            )
    
    # Handle Ishā End calculation separately.
    maghrib_str = row.get(CSVColumn.MAGHRIB.value, '').strip()
    maghrib_time = convert_to_24h_format(maghrib_str, 'maghrib')
    if not maghrib_time:
        logging.debug(f"Invalid Maghrib time '{maghrib_str}' on {event_date}")
        return

    if not next_day_row or CSVColumn.FAJR_BEGINS.value not in next_day_row:
        logging.debug(f"No next day data available for Ishā End on {event_date}.")
        return

    fajr_str = next_day_row.get(CSVColumn.FAJR_BEGINS.value, '').strip()
    fajr_time = convert_to_24h_format(fajr_str, 'fajr')
    if not fajr_time:
        logging.debug(f"Invalid Fajr time '{fajr_str}' for Ishā End calculation on {event_date}")
        return

    isha_params = calculate_isha_end(maghrib_time, fajr_time, event_date, start_date, end_date)
    if isha_params:
        start_time, end_time, target_date = isha_params
        create_event(
            service,
            "'Ishā End",
            start_time,
            end_time,
            target_date,
            Color.LAVENDER,
            dry_run
        )


def process_month(
    service: Optional[Resource],
    month_date: datetime.date,
    start_date: datetime.date,
    end_date: datetime.date,
    dry_run: bool = False
) -> datetime.date:
    """Processes prayer times for a given month."""
    current_filename = month_date.strftime(CSV_FILENAME_FORMAT).lower()
    current_data = load_csv_data(current_filename)

    # Determine next month's date
    next_month_date = (month_date.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    next_filename = next_month_date.strftime(CSV_FILENAME_FORMAT).lower()
    next_data = load_csv_data(next_filename)

    processed_dates = 0
    for current_date in sorted(current_data.keys()):
        if not (start_date <= current_date <= end_date):
            continue
        next_day = current_date + datetime.timedelta(days=1)
        next_day_row = current_data.get(next_day) or next_data.get(next_day)
        process_prayer_events(
            service,
            current_data[current_date],
            current_date,
            start_date,
            end_date,
            next_day_row,
            dry_run
        )
        processed_dates += 1
    logging.info(f"Processed {processed_dates} days from {current_filename}")
    return next_month_date


def get_validated_date(prompt: str) -> datetime.date:
    """Prompts the user for a date until a valid date is entered or attempts are exhausted."""
    for attempt in range(1, MAX_DATE_INPUT_ATTEMPTS + 1):
        date_str = input(prompt).strip()
        try:
            # Keep using DD/MM/YYYY format for user input
            return datetime.datetime.strptime(date_str, '%d/%m/%Y').date()
        except ValueError:
            print(f"Attempt {attempt}: Invalid format. Use DD/MM/YYYY.")
            logging.warning(f"Invalid date input: '{date_str}'")
    logging.critical("Maximum date input attempts exceeded.")
    sys.exit(1)


def parse_command_line_args() -> Tuple[Optional[str], Optional[str], bool]:
    """Parses command-line arguments for start and end dates and dry-run mode."""
    parser = argparse.ArgumentParser(description="Sync prayer times to Google Calendar.")
    parser.add_argument('--start-date', type=str, help="Start date in DD/MM/YYYY format.")
    parser.add_argument('--end-date', type=str, help="End date in DD/MM/YYYY format.")
    parser.add_argument('--dry-run', action='store_true', help="Process data without creating calendar events.")
    args = parser.parse_args()
    return args.start_date, args.end_date, args.dry_run


def main() -> None:
    """Main execution flow."""
    configure_logging()
    check_prayer_times_directory()

    # Parse command-line arguments
    cli_start_date, cli_end_date, dry_run = parse_command_line_args()

    # Initialize Google Calendar service if not in dry-run mode
    calendar_service = None
    if not dry_run:
        calendar_service = GoogleCalendarService().service
    else:
        logging.info("Running in dry-run mode. No calendar events will be created.")

    # Get user input if not provided via CLI
    if cli_start_date and cli_end_date:
        try:
            # Use DD/MM/YYYY format for command line arguments
            start_date = datetime.datetime.strptime(cli_start_date, '%d/%m/%Y').date()
            end_date = datetime.datetime.strptime(cli_end_date, '%d/%m/%Y').date()
        except ValueError as e:
            logging.critical(f"Invalid date format in arguments: {e}")
            sys.exit(1)
    else:
        while True:
            start_date = get_validated_date("Enter start date (DD/MM/YYYY): ")
            end_date = get_validated_date("Enter end date (DD/MM/YYYY): ")
            if end_date >= start_date:
                break
            print("End date must be on or after the start date.")
            logging.warning("End date is before start date.")

    # Process each month within the date range
    current_month = start_date.replace(day=1)
    while current_month <= end_date.replace(day=1):
        current_month = process_month(calendar_service, current_month, start_date, end_date, dry_run)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Program terminated by user.")
        sys.exit(0)
