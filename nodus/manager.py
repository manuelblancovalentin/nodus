# nodus - Job Management Framework
# Author: Manuel Blanco Valentin
# Email: manuel.blanco.valentin@gmail.com
# Created: 2024-12-25
#
# Description:
# This file contains the JobManager class responsible for managing the 
# execution of jobs, including tracking their status, job queue, and 
# interacting with the database.
#
# File: manager.py
# Purpose:
# - Contains the JobManager class for handling job queues, statuses,
#   and interacting with the Nodus database to track job progress.

# Basic modules
import os

# Time control 
import time
from datetime import datetime

# SQLite database
import sqlite3

# Import nodus
import nodus

""" Threading """
import subprocess
import threading

""" tempfile for temporary files creation """
import tempfile

""" Generic Job class """
class Job:
    def __init__(self, name, job_id, nodus_session_id, parent_caller = "nodus", pid = None, config = None, status = "pending"):
        self.name = name
        self.job_id = job_id
        self.nodus_session_id = nodus_session_id

        # Parent caller
        self.parent_caller = parent_caller if parent_caller else "nodus"

        # PID, status and config
        self.pid = pid
        self.status = status
        self.config = config

        # Init process to None (only valid after running the subprocess)
        self.process = None

        # Init markers to track progress of processes 
        self.marker_dir = tempfile.gettempdir()  # Use system's temp directory
        self.start_marker = os.path.join(self.marker_dir, f"nodus_{nodus_session_id}_{job_id}_start")
        self.end_marker = os.path.join(self.marker_dir, f"nodus_{nodus_session_id}_{job_id}_end")
        self.has_started = False  # Tracks if the start marker has been detected
        self.has_ended = False  # Tracks if the end marker has been detected

    # Even though this is defined here, it's only called in the children classes because we set the job_type there
    def _create_log_path(self):
        """Create a log file path for the job."""
        log_dir = os.path.join(nodus.__nodus_logs_dir__, self.parent_caller, self.nodus_session_id)
        os.makedirs(log_dir, exist_ok=True)
        log_name = f'job_{self.job_type}_{self.job_id}_{self.name}'
        log_name += '.log' if not self.pid else f'_pid{self.pid}.log'
        log_file = os.path.join(log_dir, log_name)
        return log_file
    
    """ This function helps us keep track of processes by creating a temporary marker file. 
        One file will be created at the start of the process, and another one at the end. 
        This, along with checking for whether the PID is running or not, allows us 
        to determine the status of the process accurately.
    """
    def _create_marker_file(self, marker_path):
        """Create a marker file to indicate job state."""
        if not os.path.exists(marker_path):
            with open(marker_path, 'w') as f:
                f.write("")
        # Log 
        nodus.__logger__.info(f"Marker file created for job {self.job_id}: {marker_path}")
        return marker_path

    def _check_job_status(self):
        """Determine the current status of a job."""
        # If the job was started as a process, check its return code
        if self.process:
            return_code = self.process.poll()  # Non-blocking check for completion
            if return_code is None:
                self.status = "running"
            else:
                self.status = "completed" if return_code == 0 else "errored"
                self.finalize()  # Create end marker
            return self.status

        # If no process, fall back to markers and PID checks
        if not self.has_started:
            if os.path.exists(self.start_marker):
                self.status = "running"
                self.has_started = True
                return self.status

        if not os.path.exists(self.start_marker):
            if nodus.utils.is_pid_running(self.pid):
                self.status = "running"
            elif os.path.exists(self.end_marker):
                self.status = "completed"
            else:
                self.status = "errored"
        return self.status
    
    def finalize(self):
        """Create end marker file."""
        self._create_marker_file(self.end_marker)

    """ Representation """
    def __repr__(self):
        s = f'<Job {self.job_id} - {self.job_type}>'
        s += f'\n    - Status: {self.status}'
        s += f'\n    - Log: {self.log_path}'
        if self.config is not None:
            if len(self.config) > 0:
                s += f'\n    - Config: '
                for key, value in self.config.items():
                    s += f'\n        - {key}: {value}'
        if self.pid:
            s += f'\n    - PID: {self.pid}'
        return s 
        

""" Command Job class """
class CommandJob(Job):
    def __init__(self, name, job_id, nodus_session_id, command = None, log_path = None, **kwargs):
        # Call super to set the basic attributes
        super().__init__(name, job_id, nodus_session_id, **kwargs)

        # Set the job type
        self.job_type = "command"

        # Set the command & shell
        self.command = command

        # Init the path
        self.log_path = log_path if log_path else self._create_log_path()
    
    def run(self):
        # Create start marker file
        start_marker = self._create_marker_file(self.start_marker)

        if self.command:
            with open(self.log_path, 'a') as log_file:
                self.process = subprocess.Popen(
                    self.command, shell=True, stdout = log_file, stderr = subprocess.STDOUT
                    )
            self.pid = self.process.pid
            # Log 
            nodus.__logger__.info(f"Command Job {self.job_id} started with PID: {self.pid}")
        else:
            nodus.__logger__.error(f"Command not provided for Job {self.job_id}.")
    
    """ Repr """
    def __repr__(self):
        s = super().__repr__()
        if self.command:
            s += f'\n    - Command: {self.command}'
        return s


""" Script Job class """
class ScriptJob(Job):
    def __init__(self, name, job_id, nodus_session_id, script_path = None, log_path = None, shell = 'bash', **kwargs):
        # Call super to set the basic attributes
        super().__init__(name, job_id, nodus_session_id, **kwargs)

        # Set the job type
        self.job_type = "script"

        # Set the script path
        self.script_path = script_path
        self.shell, self.shell_flag = self._process_shell(shell)

        # Init the path
        self.log_path = log_path if log_path else self._create_log_path()
    
    def _process_shell(self, shell):
        if shell is None:
            return 'bash', '-c'
        
        shell_flags = {
            'fish': '-e',
            'bash': '-c',
            'zsh': '-c',
            'sh': '-c',
            'ksh': '-c',
            'tcsh': '-c',
            'csh': '-c',
            'dash': '-c',  # Add support for Dash shell,
            'python': '-m'
        }
        return shell if shell in shell_flags else 'bash', shell_flags.get(shell, 'c')

    def run(self):
        # Create start marker file
        start_marker = self._create_marker_file(self.start_marker)

        if self.script_path:
            with open(self.log_path, 'a') as log_file:
                self.process = subprocess.Popen(
                    [self.shell, self.script_path], stdout = log_file, stderr = subprocess.STDOUT
                    )
            self.pid = self.process.pid
            # Log 
            nodus.__logger__.info(f"Script Job {self.job_id} started with PID: {self.pid}")
        else:
            nodus.__logger__.error(f"Script path not provided for Job {self.job_id}.")
    
    """ Repr """
    def __repr__(self):
        s = super().__repr__()
        if self.script_path:
            s += f'\n    - Script: {self.script_path}'
        if self.shell:
            s += f'\n    - Shell: {self.shell}'
        return s

""" Class for Jobs that adopt an existing PID """
class AdoptedPIDJob(Job):
    def __init__(self, name, job_id, nodus_session_id, pid = None, log_path = None, **kwargs):
        # Call super to set the basic attributes
        super().__init__(name, job_id, nodus_session_id, pid = pid, **kwargs)

        # Set the job type
        self.job_type = "pid"

        # Set the PID
        self.pid = pid

        # Init the path
        self.log_path = self._create_log_path() if log_path is None else log_path

    def run(self):
        # Create start marker file
        start_marker = self._create_marker_file(self.start_marker)

        if self.pid:
            nodus.__logger__.info(f"Adopting process with PID: {self.pid}")
            return  # No need to spawn a new process in this case.

        if self.process:
            self.pid = self.process.pid

    """ Repr """
    def __repr__(self):
        s = super().__repr__()
        return s


""" Job Manager class """
class JobManager:
    def __init__(self, name: str, db_path: str, nodus_session_id: str):
        """Initialize the JobManager with a NodusDB instance."""
        self.name = name
        self.db_path = db_path
        self.nodus_session_id = nodus_session_id

        # Create a sql connection
        self.conn = sqlite3.connect(self.db_path)
        
        # Keep track of running PIDs and job_ids
        self.running_pids = {}  
        self.jobs = {}  # Store all Job objects by job_id
        self._keys = [] # Store all job names

        # Start the monitor thread which will check the status of all jobs
        self.monitor_thread = threading.Thread(target=self._monitor_all_jobs, daemon=True)
        self.monitor_thread.start()

    def _create_job_entry(self, name, parent_caller, job_type, nodus_session_id, command = None, script_path = None, status='pending', log_path=None, pid=None, config=None, **kwargs):
        
        """Create a new job in the database and handle job execution."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Make sure pid is text
        pid = f"{pid}" if pid else None

        # Create job entry in the database
        # Create a cursor 
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO jobs (nodus_session_id, parent_caller, job_name, status, timestamp, log_path, pid, config)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (nodus_session_id, parent_caller, name, status, timestamp, log_path, pid, config))
        self.conn.commit()

        job_id = cursor.lastrowid

        # Log 
        nodus.__logger__.info(f"Job {job_id} created.")

        return job_id
    
    def create_job(self, parent_caller, job_type, name = None, nodus_session_id = None, **kwargs):
        """Create and run a new job."""
        # Check name 
        if name is None:
            name = nodus.utils.get_next_name(self.name.replace('_job_manager','_job'), self._keys)
        if nodus_session_id is None:
            nodus_session_id = self.nodus_session_id

        # Create job entry in the database
        job_id = self._create_job_entry(name, parent_caller, job_type, nodus_session_id, **kwargs)
        
        # Create the Job object
        job_class = {'command': CommandJob, 'script': ScriptJob, 'pid': AdoptedPIDJob}.get(job_type, Job)
        job = job_class(name, job_id, nodus_session_id, **kwargs)

        # Update log_path 
        if job.log_path is not None:
            kwargs['log_path'] = job.log_path
            # Update 
            self.update_log_path(job_id, job.log_path)
        
        # Run the job
        job.run()

        # Track the running PID and store the Job object
        if job.pid:
            self.running_pids[job.pid] = job_id
            # Also update pid in table 
            cursor = self.conn.cursor()
            cursor.execute('''
                UPDATE jobs
                SET pid = ?
                WHERE job_id = ?
            ''', (str(job.pid), job_id))
            self.conn.commit()
            # Log 
            nodus.__logger__.info(f"Job {job_id} started with PID: {job.pid}")

        self.jobs[job_id] = job
        self._keys.append(name)
        
        return job_id, job

    def update_job_status(self, job_id, status, job_pid, completion_time=None, db_conn = None):
        """Update the status of a job."""
        completion_time = completion_time or datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if db_conn is None:
            nodus.__logger__.info("No db connection, picking from self")
            cursor = self.conn
        else:
            cursor = db_conn

        job_pid_indicator = f"{job_pid}*" if job_pid else None

        cursor = db_conn.cursor()
        cursor.execute('''
            UPDATE jobs
            SET status = ?, completion_time = ?, pid = ?
            WHERE job_id = ?
        ''', (status, completion_time, job_pid_indicator, job_id))
        db_conn.commit()

        # Log status updated 
        nodus.__logger__.info(f"Job {job_id} status updated to {status} and PID to {job_pid_indicator}.")

    def update_log_path(self, job_id, log_path):
        """Update the log path of a job."""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE jobs
            SET log_path = ?
            WHERE job_id = ?
        ''', (log_path, job_id))
        self.conn.commit()

        # Log 
        nodus.__logger__.info(f"Job {job_id} log path updated to {log_path}.")

    def delete_job(self, job_id):
        """Delete a job from the database."""
        cursor = self.conn.cursor()
        cursor.execute('''
            DELETE FROM jobs
            WHERE job_id = ?
        ''', (job_id,))
        self.conn.commit()

        # Log job deleted
        nodus.__logger__.info(f"Job {job_id} deleted.")
    
    def kill_job(self, job_id):
        
        """Kill a running job."""
        job = self.jobs.get(job_id)
        if job is None:
            nodus.__logger__.error(f"Job {job_id} not found.")
            return False

        if job.pid is None:
            nodus.__logger__.error(f"Job {job_id} has no PID.")
            return False

        # If pid has the * at the end, skip cause this is an old PID and the process already ended
        if job.pid.endswith("*"):
            nodus.__logger__.error(f"Job {job_id} with PID {job.pid} already ended.")
            return False

        # Check if the process is running
        if not nodus.utils.is_pid_running(job.pid):
            nodus.__logger__.error(f"Job {job_id} with PID {job.pid} is not running.")
            # Update table to add the * at the end of the PID 
            cursor = self.conn.cursor()
            cursor.execute('''
                UPDATE jobs
                SET pid = ?
                WHERE job_id = ?
            ''', (f"{job.pid}*", job_id))
            self.conn.commit()
            return False

        # Kill the process
        try:
            os.kill(job.pid, 9)
            nodus.__logger__.info(f"Job {job_id} killed.")
            return True
        except Exception as e:
            nodus.__logger__.error(f"Failed to kill job {job_id}: {e}")
            return False

    # get a list of jobs 
    def get_jobs(self):
        """Get a list of all jobs."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT job_id, nodus_session_id, parent_caller, job_name, status, timestamp, completion_time, log_path, pid, config
            FROM jobs
            ORDER BY job_id DESC
        ''')
        jobs = cursor.fetchall()

        return jobs

    def get_job(self, job_id):
        """Get a specific job by its ID."""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT job_id, nodus_session_id, parent_caller, job_name, status, timestamp, completion_time, log_path, pid, config
            FROM jobs
            WHERE job_id = ?
        ''', (job_id,))
        job = cursor.fetchone()

        return job
    
    # Monitor all jobs 
    def _monitor_all_jobs(self):
        """ Note: this process is run in a separate thread,
            which means we cannot use the same db sqlite connection
            we created earlier. We need to create a new connection 
            and pass it to the update_job_status function
        """
        conn = sqlite3.connect(self.db_path)

        """Monitor all running jobs."""
        while True:
            finished_pids = []
            for job_id, job in self.jobs.items():
                if job.status in ['completed', 'errored']:
                    continue # Skip already completed jobs
                
                # check the status of the job
                current_status = job._check_job_status()

                if current_status in ['completed', 'errored']:
                    # Update the job status to completed
                    self.update_job_status(job_id, current_status, job.pid, db_conn = conn)
                    # Remove the PID from the running list
                    finished_pids.append(job.pid)

            # Remove finished jobs from the running list
            for pid in finished_pids:
                if pid in self.running_pids:
                    del self.running_pids[pid]

            time.sleep(1)
    
