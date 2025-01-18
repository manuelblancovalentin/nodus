""" Basic modules """
import os

""" tempfile for temporary files creation """
import tempfile

""" Threading """
import subprocess

""" Nodus """
import nodus

""" Generic Job class """
class Job:
    def __init__(self, name, job_id, nodus_session_id, parent_caller = "nodus", pid = None, config = None, 
                 status = "pending", priority = 0 ):
        self.name = name
        self.job_id = job_id
        self.nodus_session_id = nodus_session_id

        # Parent caller
        self.parent_caller = parent_caller if parent_caller else "nodus"

        # PID, status and config
        self.pid = pid
        self.status = status
        self.config = config
        self.priority = priority

        # Init process to None (only valid after running the subprocess)
        self.process = None
        self.dependencies = set() # Empt set to store dependencies

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
        #nodus.__logger__.debug(f"Checking status of Job {self.name} [{self.job_id}] with PID: {self.pid}: {self.status}")
        
        # If the job was started as a process, check its return code
        if self.process and self.has_started:
            return_code = self.process.poll()  # Non-blocking check for completion
            #nodus.__logger__.debug(f"\t{self.pid}, HAS STARTED and has a process {self.process}. Return code is: {return_code}")
            if return_code is None:
                self.status = "running"
            else:
                self.status = "completed" if return_code == 0 else "errored"
                self.finalize()  # Create end marker
                nodus.__logger__.info(f"Job {self.name} [{self.job_id}] finalized with status: {self.status}")
            return self.status
        
        # If the job is in 'waiting' state, ensure it does not transition to 'errored'
        if self.status == "waiting" or self.status == 'pending':
            # Only allow 'waiting' jobs to transition once dependencies are resolved
            return self.status

        # If no process, fall back to markers and PID checks
        if not self.has_started and os.path.exists(self.start_marker):
            self.status = "running"
            self.has_started = True
            #nodus.__logger__.debug(f"Job {self.name} [{self.job_id}] marked as started since start marker exists.")
            return self.status
        
        if not os.path.exists(self.start_marker):
            if nodus.utils.is_pid_running(self.pid):
                self.status = "running"
            elif os.path.exists(self.end_marker):
                self.status = "completed"
            elif self.status != 'waiting' and self.status != 'pending':
                self.status = "errored"

        if self.status == "completed":
            self.finalize()

        #nodus.__logger__.debug(f"Job {self.name} [{self.job_id}] status checked: {self.status}")

        return self.status
    
    def finalize(self):
        """Create end marker file."""
        self._create_marker_file(self.end_marker)
        self.has_ended = True

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
            self.has_started = True
            # Log 
            nodus.__logger__.info(f"Command Job {self.name} [{self.job_id}] started with PID: {self.pid}")
        else:
            nodus.__logger__.error(f"Command not provided for Job {self.name} [{self.job_id}]")

        # Update status
        self.status = self._check_job_status()

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
            self.has_started = True
            # Log 
            nodus.__logger__.info(f"Script Job {self.job_id} started with PID: {self.pid}")
        else:
            nodus.__logger__.error(f"Script path not provided for Job {self.job_id}.")
    
        # Update status
        self.status = self._check_job_status()

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
        self.has_started = True

        if self.pid:
            nodus.__logger__.info(f"Adopting process with PID: {self.pid}")
            return  # No need to spawn a new process in this case.

        if self.process:
            self.pid = self.process.pid
        
        # Update status
        self.status = self._check_job_status()

    """ Repr """
    def __repr__(self):
        s = super().__repr__()
        return s
