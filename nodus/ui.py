import curses
import sqlite3
import os
from datetime import datetime
import time

import nodus

# Import subprocess to run 'less' command
import subprocess

def curses_main(stdscr):
    """Main curses function."""
    curses.curs_set(0)  # Hide the cursor
    stdscr.nodelay(True)  # Make getch() non-blocking
    stdscr.clear()

    # Colors
    curses.start_color()
    curses.init_pair(1, curses.COLOR_GREEN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    curses.init_pair(4, curses.COLOR_CYAN, curses.COLOR_BLACK)

    # Initialize a nodus session
    session = nodus.NodusSession()
    db = session.add_nodus_db()  # Add a default NodusDB instance

    # Get the JobManager from the db instance
    jm = db.job_manager

    current_row = 0
    last_update_time = datetime.now()
    current_time = last_update_time.strftime('%H:%M:%S')
    header = f"Nodus Job Manager (Standalone Mode) - {current_time}"

    while True:
        # Periodically refresh the header with the current time
        now = datetime.now()
        if (now - last_update_time).seconds >= 1:
            last_update_time = now
            current_time = now.strftime('%H:%M:%S')
            header = f"Nodus Job Manager (Standalone Mode) - {current_time}"

        # Get jobs
        jobs = jm.get_jobs()

        # Clear and redraw the main screen
        stdscr.clear()
        height, width = stdscr.getmaxyx()

        # Header
        stdscr.attron(curses.color_pair(4))
        stdscr.addstr(0, width // 2 - len(header) // 2, header)
        stdscr.attroff(curses.color_pair(4))

        # Instructions
        instructions = "Use UP/DOWN to navigate, ENTER to select, D to delete, K to kill, L to open log, Q to quit"
        stdscr.addstr(1, 0, instructions, curses.color_pair(3))

        # Display jobs
        if jobs:
            stdscr.addstr(3, 0, f"{'ID':<5} {'PID':<8} {'NODUS_SESSION_ID':<40} {'PARENT CALLER':<30} {'JOB NAME':<15} {'STATUS':<15} {'START TIME':<20} {'END TIME':<20} {'RUNTIME':<15} {'LOG':<30}", curses.A_BOLD)
            for idx, (job_id, nodus_session_id, parent_caller, job_name, status, timestamp, completion_time, log_path, pid, config) in enumerate(jobs):
                # If completion_time is None, display "N/A"
                completion_time = completion_time if completion_time else "N/A"
                # If PID is None, display "N/A"
                pid = pid if pid else "N/A"
                # If parent_caller is None, display "N/A"
                parent_caller = parent_caller if parent_caller else "N/A"
                # If parent_caller is too long, truncate it
                parent_caller = parent_caller[:27] + "..." if len(parent_caller) > 30 else parent_caller
                # If job_name is None, display "N/A"
                job_name = job_name if job_name else "N/A"
                # If job_name is too long, truncate it
                job_name = job_name[:12] + "..." if len(job_name) > 15 else job_name
                # If endtime is not N/A, calculate runtime
                runtime = "N/A"
                if completion_time != "N/A" and timestamp:
                    runtime = nodus.utils.compute_runtime(timestamp, completion_time)
                # If log_path is None, N/A
                log_path = log_path if log_path else "N/A"
                # If log_path is too long, truncate it
                if len(log_path) > 30:
                    lp = log_path.split(os.sep)[-1]
                    log_path = f'…{os.sep}{lp[:15]}…{lp[-14:]}' if len(lp) > 30 else f"…{os.sep}{lp}"
                #log_path = log_path[:17] + "..." if len(log_path) > 20 else log_path
                # Highlight the current row
                if idx == current_row:
                    stdscr.attron(curses.color_pair(1))
                    stdscr.addstr(4 + idx, 0, f"{job_id:<5} {pid:<8} {nodus_session_id:<40} {parent_caller:<30} {job_name:<15} {status:<15} {timestamp:<20} {completion_time:<20} {runtime:<15} {log_path:<30}")
                    stdscr.attroff(curses.color_pair(1))
                else:
                    stdscr.addstr(4 + idx, 0, f"{str(job_id):<5} {str(pid):<8} {nodus_session_id:<40} {parent_caller:<30} {job_name:<15} {status:<15} {timestamp:<20} {completion_time:<20} {runtime:<15} {log_path:<30}")
        else:
            stdscr.addstr(3, 0, "No jobs found.", curses.color_pair(2))

        stdscr.refresh()

        # Handle key inputs
        try:
            key = stdscr.getch()
        except:
            key = None

        if key == curses.KEY_UP and current_row > 0:
            current_row -= 1
        elif key == curses.KEY_DOWN and current_row < len(jobs) - 1:
            current_row += 1
        elif key == ord("q"):
            break
        elif key == ord("d") and jobs:
            # Delete the selected job entry from the database
            selected_job = jobs[current_row]
            confirm = display_confirmation_popup(stdscr, "Delete this job entry from database? This will NOT kill the process. (y/n)")
            if confirm:
                jm.delete_job(selected_job[0])
        elif key == ord("k") and jobs:
            selected_job = jobs[current_row]
            # Get PID 
            selected_pid = selected_job[8]
            # if PID is N/A or contains an asterisk, tell the user this process cannot be killed 
            skip = False
            if selected_pid == 'N/A':
                skip = True
                display_message_notification(stdscr, "This process cannot be killed. PID is NULL.")
            if selected_pid.endswith('*'):
                display_message_notification(stdscr, "This process cannot be killed since the process already finished (note the * next to the PID number).")
                skip = True
            
            if not skip:
                confirm = display_confirmation_popup(stdscr, f"Kill this job with PID {selected_pid}? This will NOT delete entry from database. (y/n)")
                if confirm:
                    process_killed = nodus.utils.kill_process(selected_pid)
                    # If process_killed is False, show an error message to user
                    # Make sure to delete the previous confirmation popup 
                    stdscr.clear()
                    if not process_killed: display_message_notification(stdscr, "Failed to kill the process!")

        elif key == ord("l") and jobs:
            selected_job = jobs[current_row]
            log_path = selected_job[7]
            if log_path is None:
                display_message_notification(stdscr, f"Log file is null.")
            else:
                if os.path.exists(log_path):
                    # Check if process is still running, if it is, open with tail -F, else open with less
                    status = selected_job[4]
                    if status in ['completed','error','killed']:
                        # Just open file in less
                        open_file_in_less(stdscr, log_path)
                    else:
                        open_file_in_tail(stdscr, log_path)
                else:
                    stdscr.clear()
                    display_message_notification(stdscr, f"Log file {log_path} not found!")
        elif key == ord("\n") and jobs:
            selected_job = jobs[current_row]
            handle_job_selection(stdscr, selected_job)

        time.sleep(0.1)  # Small delay to avoid high CPU usage

def display_confirmation_popup(stdscr, message):
    """Display a confirmation popup."""
    height, width = stdscr.getmaxyx()
    popup_height = 3
    popup_width = len(message) + 4
    popup_y = height // 2 - popup_height // 2
    popup_x = width // 2 - popup_width // 2

    # Draw the popup
    stdscr.attron(curses.color_pair(3))
    stdscr.addstr(popup_y, popup_x, f"+{'-' * (popup_width - 2)}+")
    stdscr.addstr(popup_y + 1, popup_x, f"| {message} |")
    stdscr.addstr(popup_y + 2, popup_x, f"+{'-' * (popup_width - 2)}+")
    stdscr.attroff(curses.color_pair(3))
    stdscr.refresh()

    while True:
        key = stdscr.getch()
        if key == ord("y"):
            return True
        elif key == ord("n"):
            return False
        
def display_message_notification(stdscr, message):
    """Display a message notification."""
    height, width = stdscr.getmaxyx()
    popup_height = 3
    popup_width = len(message) + 4
    popup_y = height // 2 - popup_height // 2
    popup_x = width // 2 - popup_width // 2

    # Draw the popup
    stdscr.attron(curses.color_pair(3))
    stdscr.addstr(popup_y, popup_x, f"+{'-' * (popup_width - 2)}+")
    stdscr.addstr(popup_y + 1, popup_x, f"| {message} |")
    stdscr.addstr(popup_y + 2, popup_x, f"+{'-' * (popup_width - 2)}+")
    stdscr.attroff(curses.color_pair(3))
    stdscr.refresh()
    
    while True:
        # Wait until ENTER
        key = stdscr.getch()
        if key == ord("\n"):
            break

def handle_job_selection(stdscr, job):
    """Handle selection of a job."""
    stdscr.clear()
    height, width = stdscr.getmaxyx()

    # Display job details
    title = f"Job Details (ID: {job[0]})"
    stdscr.attron(curses.color_pair(4))
    stdscr.addstr(0, width // 2 - len(title) // 2, title)
    stdscr.attroff(curses.color_pair(4))

    details = [
        f"Nodus session ID: {job[1]}",
        f"Parent Caller: {job[2]}",
        f"Job Name: {job[3]}",
        f"Status: {job[4]}",
        f"Start Time: {job[5]}",
        f"End Time: {job[6] if job[6] else 'N/A'}",
        f"Log Path: {job[7]}",
        f"PID: {job[8]}",
        f"Config: {job[9]}",
    ]
    

    for idx, line in enumerate(details):
        stdscr.addstr(2 + idx, 0, line)

    instructions = "Press B or ENTER to go back | D to delete job | K to kill job | L to open log"
    stdscr.addstr(height - 2, 0, instructions, curses.color_pair(3))
    stdscr.refresh()

    while True:
        key = stdscr.getch()
        if key == ord("b") or key == ord("\n"):
            break
        elif key == ord("k"):
            confirm = display_confirmation_popup(stdscr, "Kill this job? (y/n)")
            if confirm:
                delete_job(job[0])
                break
        elif key == ord("l"):
            log_path = job[7]
            if log_path is not None:
                if os.path.exists(log_path):
                    open_file_in_less(stdscr, log_path)
                else:
                    stdscr.addstr(height - 3, 0, "Log file not found!", curses.color_pair(2))
                    stdscr.refresh()
                    time.sleep(1)
            else:
                stdscr.addstr(height - 3, 0, "Log file is null!", curses.color_pair(2))
                stdscr.refresh()
                time.sleep(1)


def open_file_in_less(stdscr, file):
    """Open the file in 'less' and return to the curses UI cleanly."""
    curses.endwin()  # Restore the terminal to its normal state
    try:
        subprocess.run(['less', file])  # Run 'less' on the file
    finally:
        curses.doupdate()  # Reinitialize the curses screen

def open_file_in_tail(stdscr, file):
    """Open the file in 'tail -F' and return to the curses UI cleanly.
        We need a new subprocess cause issuing a Control+C to kill tail
        will also kill the curses UI.
    """
    curses.endwin()  # Restore the terminal to its normal state
    try:
        subprocess.Popen(
            ["tail", "-F", file],
            preexec_fn=os.setpgrp  # Start a new process group
        ).wait()
    except KeyboardInterrupt:
        pass
    finally:
        curses.doupdate()  # Reinitialize the curses screen

    
def run_ui():
    curses.wrapper(curses_main)
