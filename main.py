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
CSV_DATE_FORMAT = '%d/%m/%Y'
MAX_DATE_INPUT_ATTEMPTS = 5
CSV_FILENAME_FORMAT = '%b-%Y.csv'  # e.g. 'apr-2025.csv'
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'
LOG_FILE = 'calendar_sync.log'


# Column Names
class CSVColumn(Enum):
    DATE = 'Date'
    FAJR_BEGINS = 'Fajr Begins'
    DHUR_JAMAT = 'Dhur Jamat'
    ASR_JAMAT = 'Asr Jamat'
    MAGHRIB = 'Maghrib'
    ISHA_JAMAT = 'Isha Jamat'


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
    EventConfig('Tahajjud', CSVColumn.FAJR_BEGINS, -40, 0),
    EventConfig('Fajr & Morning Adhkār', CSVColumn.FAJR_BEGINS, 0, 45),
    EventConfig('Lunch, News, Ẓuhr & Habits', CSVColumn.DHUR_JAMAT, -30, 60),
    EventConfig("'Aṣr & Evening Adhkār", CSVColumn.ASR_JAMAT, -15, 30),
    EventConfig('Maghrib', CSVColumn.MAGHRIB, -10, 35),
    EventConfig("'Ishā & Qur'ān", CSVColumn.ISHA_JAMAT, -15, 30),
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
        return datetime.datetime.strptime(time_str, '%H:%M').time()
    except ValueError as e:
        logging.error(f"Invalid time format '{time_str}': {e}")
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
    try:
        with open(filepath, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                date_str = row.get(CSVColumn.DATE.value, '').strip()
                try:
                    row_date = datetime.datetime.strptime(date_str, CSV_DATE_FORMAT).date()
                    data[row_date] = row
                except ValueError:
                    logging.warning(f"Invalid date format in row: '{date_str}'")
        logging.debug(f"Loaded data from {filename} with {len(data)} entries.")
    except FileNotFoundError:
        logging.warning(f"Missing CSV file: {filepath}")
    return data


def create_event(
    service: Resource,
    summary: str,
    start_time: datetime.time,
    end_time: datetime.time,
    event_date: datetime.date,
    color: Color
) -> bool:
    """Creates a Google Calendar event."""
    try:
        start_dt = datetime.datetime.combine(event_date, start_time, tzinfo=TIME_ZONE)
        end_dt = datetime.datetime.combine(event_date, end_time, tzinfo=TIME_ZONE)
        if end_dt <= start_dt:
            end_dt += datetime.timedelta(days=1)
        event_body = {
            'summary': summary,
            'start': {'dateTime': start_dt.isoformat(), 'timeZone': TIME_ZONE.key},
            'end': {'dateTime': end_dt.isoformat(), 'timeZone': TIME_ZONE.key},
            'colorId': color.value
        }
        service.events().insert(calendarId='primary', body=event_body).execute()
        logging.info(f"Created event: '{summary}' on {event_date.strftime(CSV_DATE_FORMAT)}")
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


def process_prayer_events(
    service: Resource,
    row: Dict[str, str],
    event_date: datetime.date,
    start_date: datetime.date,
    end_date: datetime.date,
    next_day_row: Optional[Dict[str, str]]
) -> None:
    """Processes and creates prayer time events for a specific date."""
    # Process each event from the configuration
    for config in EVENT_CONFIGS:
        time_str = row.get(config.csv_column.value, '').strip()
        if not time_str:
            logging.debug(f"No time data for '{config.summary}' on {event_date}")
            continue

        # Special handling for Tahajjud
        if config.summary == 'Tahajjud':
            base_time = parse_time(time_str)
            if not base_time:
                continue
            base_dt = datetime.datetime.combine(event_date, base_time)
            # Start: 40 minutes before Fajr Begins, rounded to nearest 5 minutes.
            start_time = round_time((base_dt + datetime.timedelta(minutes=-40)).time(), 5)
            # End: exactly Fajr Begins (no rounding)
            end_time = base_time

        # Special handling for Fajr
        elif config.summary.startswith('Fajr'):
            base_time = parse_time(time_str)
            if not base_time:
                continue
            # Start: exactly Fajr Begins (no rounding)
            start_time = base_time
            # End: 45 minutes after Fajr Begins, rounded to the nearest 15 minutes.
            end_dt = datetime.datetime.combine(event_date, base_time) + datetime.timedelta(minutes=45)
            end_time = round_time(end_dt.time(), 15)

        # All other events: use the configured offsets and round to the nearest 15 minutes.
        else:
            start_time = adjust_time_custom(time_str, config.start_adjust, 15)
            end_time = adjust_time_custom(time_str, config.end_adjust, 15)

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
            Color.LAVENDER
        )

    # Handle Ishā End calculation separately.
    maghrib_str = row.get(CSVColumn.MAGHRIB.value, '').strip()
    maghrib_time = parse_time(maghrib_str)
    if not maghrib_time:
        logging.debug(f"Invalid Maghrib time '{maghrib_str}' on {event_date}")
        return

    if not next_day_row or CSVColumn.FAJR_BEGINS.value not in next_day_row:
        logging.debug(f"No next day data available for Ishā End on {event_date}.")
        return

    fajr_str = next_day_row.get(CSVColumn.FAJR_BEGINS.value, '').strip()
    fajr_time = parse_time(fajr_str)
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
            Color.LAVENDER
        )


def process_month(
    service: Resource,
    month_date: datetime.date,
    start_date: datetime.date,
    end_date: datetime.date
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
            next_day_row
        )
        processed_dates += 1
    logging.info(f"Processed {processed_dates} days from {current_filename}")
    return next_month_date


def get_validated_date(prompt: str) -> datetime.date:
    """Prompts the user for a date until a valid date is entered or attempts are exhausted."""
    for attempt in range(1, MAX_DATE_INPUT_ATTEMPTS + 1):
        date_str = input(prompt).strip()
        try:
            return datetime.datetime.strptime(date_str, CSV_DATE_FORMAT).date()
        except ValueError:
            print(f"Attempt {attempt}: Invalid format. Use DD/MM/YYYY.")
            logging.warning(f"Invalid date input: '{date_str}'")
    logging.critical("Maximum date input attempts exceeded.")
    sys.exit(1)


def parse_command_line_args() -> Tuple[Optional[str], Optional[str]]:
    """Parses command-line arguments for start and end dates."""
    parser = argparse.ArgumentParser(description="Sync prayer times to Google Calendar.")
    parser.add_argument('--start-date', type=str, help=f"Start date in {CSV_DATE_FORMAT} format.")
    parser.add_argument('--end-date', type=str, help=f"End date in {CSV_DATE_FORMAT} format.")
    args = parser.parse_args()
    return args.start_date, args.end_date


def main() -> None:
    """Main execution flow."""
    configure_logging()
    check_prayer_times_directory()

    # Initialize Google Calendar service
    calendar_service = GoogleCalendarService().service

    # Parse command-line arguments
    cli_start_date, cli_end_date = parse_command_line_args()

    # Get user input if not provided via CLI
    if cli_start_date and cli_end_date:
        try:
            start_date = datetime.datetime.strptime(cli_start_date, CSV_DATE_FORMAT).date()
            end_date = datetime.datetime.strptime(cli_end_date, CSV_DATE_FORMAT).date()
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
        current_month = process_month(calendar_service, current_month, start_date, end_date)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Program terminated by user.")
        sys.exit(0)
