# nodus - Job Management Framework
# Author: Manuel Blanco Valentin
# Email: manuel.blanco.valentin@gmail.com
# Created: 2024-12-25
#
# Description:
# This file contains the some utilities for the Nodus job management framework.
#
# File: utils.py
# Purpose:
# - Contains some basic functionalities used by the Nodus framework.

""" Basic imports """
import os
import re

""" Loggers """
import logging
from datetime import datetime

""" Subprocess to check if pd is running using ps (if not linux) """
import subprocess

""" Platform to determine the current system """
import platform

""" Numpy """
import numpy as np

""" Import nodus """
import nodus

""" Printers """
import logging
from datetime import datetime


""" Get nodus version """
def get_nodus_version():
    """Get the version of the Nodus framework."""
    return nodus.__version__


class CustomFormatter(logging.Formatter):
    def __init__(self, *args, level_formatter = "{level_name}", time_formatter = "{current_time:<8}", **kwargs):
        super().__init__(*args, **kwargs)
        self.last_level = None
        self.level_formatter = level_formatter
        self.time_formatter = time_formatter
        self.print_level = True

    def format(self, record):

        # Get the current time
        current_time = datetime.now().strftime('%H:%M:%S.%f')[:-4]

        # Format the log level and message
        level_name = record.levelname.replace('WARNING', 'WARN').replace('ERROR', 'ERR')
        message = record.getMessage()

        # Check if level changed 
        #print_level = self.last_level != level_name
        #self.last_level = level_name            

        # Format the log level and time 
        level_name = self.level_formatter.format(level_name = level_name)
        current_time = self.time_formatter.format(current_time = current_time)

        # Prepare the basic log entry (level, time, message)
        log_entry = f'╰ {level_name}┤ {current_time} │ - {message}'

        if self.print_level:
            return f'{log_entry}'
        else:
            # Otherwise, just print the time with a space for level
            #level_name = f"{'':<5}"  # Empty string for level
            level_name = self.level_formatter.format(level_name = "")
            space = ' ' * (len(level_name))  # Space for level
            return f' {space} │ {current_time} │ - {message}'

""" Custom logger """
class CustomLogger:
    def __init__(self, dir):
        self.last_level = None
        self.last_date = None
        self.formatter = None
        self.log_file_path = None

        # Get the maximum length allowed for the log message (which means, for the level name, and the date)
        self.__get_max_lengths()

        # Set up logger with both console and file logging
        self.logger = self.setup_logger(dir = dir, level_formatter = self.fmt_level, time_formatter = self.fmt_time, log_to_file=True)

    def __get_max_lengths(self):
        level_names = ['INFO', 'WARN', 'ERR', 'DBG', 'CRIT', 'LOG']
        self.max_level_length = max(max([len(level) for level in level_names]), 5)
        class fmt_level:
            def __init__(self, max_length):
                self.max_length = max_length
            def format(self, level_name = ""):
                return level_name + ' ' + (self.max_length-len(level_name))*"─"
        self.fmt_level = fmt_level(self.max_level_length)
        self.fmt_time = "{current_time:<8}"
    
    def setup_logger(self, dir = '.', log_to_file=False, log_file_path=None, **kwargs):
        # Set up the logger
        logger = logging.getLogger('nodus')
        logger.setLevel(logging.DEBUG)

        # Formatter for the log messages
        self.formatter = CustomFormatter('%(message)s', **kwargs)

        # Console handler to print to stdout
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(self.formatter)
        logger.addHandler(console_handler)

        # File handler to log to a file, if enabled
        if log_to_file:
            log_file_path = log_file_path or os.path.join(dir, f'nodus_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
            self.log_file_path = log_file_path
            file_handler = logging.FileHandler(log_file_path)
            file_handler.setFormatter(self.formatter)
            print(f'Logging to file: {log_file_path}')
            logger.addHandler(file_handler)

        return logger

    # Function to get the current date and time
    def __get_datetime(self):
        # Get the current date and time
        current_date = datetime.now().strftime('%d/%b/%Y')
        current_time = datetime.now().strftime('%H:%M:%S.%f')[:-4]
        return current_date, current_time

    # Function to change the formatter of the logger handlers
    def __change_handlers_formatter(self, formatter):
        # Loop thru logger handlers
        for handler in self.logger.handlers:
            handler.setFormatter(formatter)

    def __check_date(self, current_date):
        # Handle date change
        print_cross = True
        if self.last_date != current_date:
            date_change = f'-- Date: {current_date}'
            self.last_date = current_date
            # Set None formatters just to print the date 
            self.__change_handlers_formatter(None)
            # Print the date
            self.logger.info(date_change)
            # Set the formatter back
            fmt = CustomFormatter('%(message)s', level_formatter = self.fmt_level, time_formatter = self.fmt_time)
            self.__change_handlers_formatter(fmt)
            # Assign the variable print_cross to False 
            print_cross = False
        return print_cross

    def __check_level(self, level_name, print_cross = True, print_header = False):
        # Check if level changed 
        old_level = str(self.last_level)
        print_top_border = self.last_level != level_name | print_header
        self.last_level = level_name

        # Define border characters
        bchar = ['┬','┼']
        bchar3 = ['╮','┤']

        # If we need to print the top border, do it here
        if print_top_border:
            # Draw a box around the log entry for the first message of the level
            border_top = '╭' + '─' * (self.max_level_length + 2) + bchar[int(print_cross)] + '─' * (8 + 2 + 3) + bchar3[int(print_cross)]
            # Set None formatters just to print the border
            self.__change_handlers_formatter(None)
            # Print the border
            self.logger.info(border_top)
            # Set the formatter back
            fmt = CustomFormatter('%(message)s', level_formatter = self.fmt_level, time_formatter = self.fmt_time)
            fmt.print_level = True
            self.__change_handlers_formatter(fmt)
        else:
            # Set the formatter back
            fmt = CustomFormatter('%(message)s', level_formatter = self.fmt_level, time_formatter = self.fmt_time)
            fmt.print_level = False
            self.__change_handlers_formatter(fmt)
        return print_top_border

    def _preprints(self, level, print_header = False):
        # Get the current date and time
        current_date, current_time = self.__get_datetime()
        print_cross = self.__check_date(current_date)

        # Check if the level changed. If it did, we need to print the top border
        print_top_border = self.__check_level(level, print_cross, print_header)

    def info(self, msg, print_header = False, **kwargs):
        self._preprints('INFO', print_header = print_header)
        self.logger.info(msg, **kwargs)

    def error(self, msg, print_header = False, **kwargs):
        self._preprints('ERR', print_header = print_header)
        self.logger.error(msg, **kwargs)

    def warn(self, msg, print_header = False, **kwargs):
        self._preprints('WARN', print_header = print_header)
        self.logger.warning(msg, **kwargs)
    
    def debug(self, msg, print_header = False, **kwargs):
        self._preprints('DBG', print_header = print_header)
        self.logger.debug(msg, **kwargs)
    
    def critical(self, msg, print_header = False, **kwargs):
        self._preprints('CRIT', print_header = print_header)
        self.logger.critical(msg, **kwargs)
    
    def custom(self, level, msg, print_header = False, **kwargs):
        # Ensure level is at max 4 characters
        level = level[:4]
        self._preprints(level, print_header = print_header)
        self.logger.log(msg, level = level, **kwargs)
    
    def close(self):
        level_name = self.fmt_level.format(level_name = "")
        space = ' ' * (len(level_name) + 2)  # Space for level
        # Print the last bottom border 
        border_bottom = space + '╰' + '─' * (8 + 5) + '╯'
        # Set formatters to None
        self.__change_handlers_formatter(None)
        # Print the bottom border
        self.logger.info(border_bottom)
        # Print a goodbye message with the current date and time
        current_date = datetime.now().strftime('%d/%b/%Y')
        current_time = datetime.now().strftime('%H:%M:%S.%f')[:-4]
        self.logger.info(f'-- Nodus closing @ [{current_date} {current_time}] 👋')
        # Set the formatter back
        fmt = CustomFormatter('%(message)s', level_formatter = self.fmt_level, time_formatter = self.fmt_time)
        # Done
        self.__change_handlers_formatter(fmt)


""" Setup a header to print when nodus starts """
def get_header():
    sp = 15
    s = []
    prob = 0.5
    for _ in range(3):
        ss = " "
        for i in range(sp):
            # Generate random number betwen 2190 and 21ff in hex 
            #
            # check if random prob is less than 0.3
            if np.random.rand() <= prob:
                #j = np.random.randint(0x2190, 0x25ff)
                #j = np.random.randint(0x25a0, 0x25ff)
                #j = np.random.randint(0x239b, 0x23a0)
                #j = np.random.randint(0x2500, 0x257f)
                j = np.random.randint(0x2596, 0x259f)
                #j = 0x232c
                ss += chr(j)
                #ss += "∘"
            else:
                ss += " "

        ss += " "
        s.append(ss)
    HEADER  = f"\x1B[48;2;141;47;102m\x1B[48;2;141;47;102m \x1B[48;2;117;39;85m \x1B[48;2;94;31;68m \x1B[48;2;70;23;51m \x1B[48;2;46;15;33m \x1B[48;2;23;7;17m \x1B[48;2;0;0;0m \x1B[48;2;23;7;17m \x1B[48;2;46;15;33m \x1B[48;2;70;23;51m \x1B[48;2;94;31;68m \x1B[48;2;117;39;85m \x1B[48;2;141;47;102m      \x1b[48;2;249;230;207m{s[0]}\x1B[0m\n"
    HEADER += f"\x1B[48;2;141;47;102m\x1B[48;2;141;47;102m \x1B[48;2;117;39;85m \x1B[48;2;94;31;68m \x1B[48;2;70;23;51m𐌍\x1B[48;2;46;15;33m \x1B[48;2;23;7;17m↠\x1B[48;2;0;0;0m⌾\x1B[48;2;23;7;17m↞\x1B[48;2;46;15;33m \x1B[48;2;70;23;51m𐌃\x1B[48;2;94;31;68m \x1B[48;2;117;39;85m𐌖\x1B[48;2;141;47;102m 𐌔    \x1B[48;2;250;198;122m{s[1]}\x1B[0m\n"
    HEADER += f"\x1B[48;2;141;47;102m\x1B[48;2;141;47;102m \x1B[48;2;117;39;85m \x1B[48;2;94;31;68m \x1B[48;2;70;23;51m \x1B[48;2;46;15;33m \x1B[48;2;23;7;17m \x1B[48;2;0;0;0m \x1B[48;2;23;7;17m \x1B[48;2;46;15;33m \x1B[48;2;70;23;51m \x1B[48;2;94;31;68m \x1B[48;2;117;39;85m \x1B[48;2;141;47;102m      \x1B[48;2;250;198;122m{s[2]}\x1B[0m\n"
    return HEADER

""" Get the next available name for a nodus database """
def get_next_name(base, existing_names):
    """Get the next available name for a nodus database."""
    # Get the next available name (nodus_db_1, nodus_db_2, ...)
    # even if an index doesn't exist (like if nodus_db_2 is there, but nodus_db_1 is not)
    # we should always pick the next available index (in this case 3, even though 1 isn't there)
    if base is None:
        base = 'nodus_db'
    if existing_names is None:
        existing_names = []

    # Create a regex pattern to match the base name
    pat = base.replace('_','\_')
    pat += r"\_?(\d+)?"
    pat = re.compile(pat)

    # Now let's find the highest number in the entries
    max_num = 0
    for entry in existing_names:
        match = pat.match(entry)
        if match:
            num = match.group(1)
            if num is not None:
                num = int(num)
                max_num = max(max_num, num)

    if max_num == 0:
        return base
    elif max_num > 0:
        return f"{base}_{max_num + 1}"

# Check if a process is running 
def can_pid_be_killed(pid):
    """Check if a process with the given PID is running."""
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return

def is_pid_running_proc(pid):
    """Check if a process with the given PID is running using /proc."""
    return os.path.exists(f"/proc/{pid}")

def is_pid_running_ps(pid):
    """Check if a process with the given PID is running using 'ps'."""
    try:
        result = subprocess.run(['ps', '-p', str(pid)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0  # Return 0 means the process exists
    except Exception as e:
        print(f"[ERROR] Failed to check PID using 'ps': {e}")
        return False

# Define function is_process_running according to OS 
def check_platform():
    system = platform.system()

    if system == "Darwin":
        return "macOS"
    elif system == "Windows":
        return "Windows"
    elif system == "Linux":
        return "Linux"
    else:
        return "Unknown"

# Current system
current_system = check_platform()

is_pid_running = {'Linux': is_pid_running_proc, 'macOS': is_pid_running_ps, 'Windows': is_pid_running_ps}.get(current_system, is_pid_running_ps)


def kill_process(pid):
    # Check if the process is running
    if is_pid_running(pid):
        nodus.__logger__.error(f"Attempted to kill PID {pid}, but it is not running.")

    # Kill the process
    try:
        os.kill(pid, 9)
        nodus.__logger__.info(f"Job running at PID {pid} killed.")
        return True
    except Exception as e:
        nodus.__logger__.error(f"Failed to kill job at PID {pid}: {e}")
        return False


def compute_runtime(start_time, end_time):
    start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
    runtime = end_time - start_time

    # Now format the runtime into <number_of_days> days, HH:MM:SS
    days = runtime.days
    hours, remainder = divmod(runtime.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if days == 0:
        return f"{hours:02}:{minutes:02}:{seconds:02}"
    else:
        return f"{days} days, {hours:02}:{minutes:02}:{seconds:02}"