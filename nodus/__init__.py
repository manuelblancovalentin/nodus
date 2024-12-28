# Basic imports 
import os 

""" First of all, let's setup the directory where nodus is located """
__nodus_bin_dir__ = os.path.dirname(os.path.abspath(__file__))
__version__ = "0.1.0"

""" Also get the user's home directory where we will store data and also logs """
__nodus_data_dir__ = os.path.expanduser('~/.nodus')
# Make sure dir exists 
try:
    os.makedirs(__nodus_data_dir__, exist_ok=True)
except Exception as e:
    logging.error(f"Failed to create data directory: {e}")

# Setup db path
__nodus_db_path__ = os.path.join(__nodus_data_dir__, 'nodus.db')

""" Import utils """
from .utils import CustomLogger, get_header
# Print header
HEADER = get_header()
print(HEADER)

""" Setup logger """
__logger__ = CustomLogger(dir = __nodus_data_dir__)
__logger__.info("Nodus initialized")
__logger__.info(f"Nodus version: {__version__}")

# Get log file path
__nodus_log_file__ = __logger__.log_file_path

# We can initialize the log dir (where we will put the logs for each job, in case the user doesn't pass one)
__nodus_logs_dir__ = os.path.join(__nodus_data_dir__, 'logs')
if not os.path.exists(__nodus_logs_dir__):
    os.makedirs(__nodus_logs_dir__)
    __logger__.info(f"Created logs directory: {__nodus_logs_dir__}")

# Import nodus
from .db import NodusSession, NodusDB
__logger__.info("Nodus imported")
# Import JobManager
from .manager import JobManager, Job
__logger__.info("JobManager imported")
__logger__.info("Nodus ready to use")

# Import ui if needed
try:
    from .ui import curses_main
except ImportError as e:
    __logger__.error(f"Failed to import UI module: {e}")

""" At exit to determine when the program is closing """
import atexit

""" Register the final logging message when exiting the program """
def log_before_exit():
    __logger__.close()

# Register the function to be called on exit
atexit.register(log_before_exit)
