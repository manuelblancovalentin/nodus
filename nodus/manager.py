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

# typing
from typing import List

# Time control 
import time
from datetime import datetime

# SQLite database
import sqlite3

# Import nodus
import nodus

from .job import Job, CommandJob, ScriptJob, AdoptedPIDJob

""" Threading """
import threading


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
        self.pending_jobs = {}  # Store jobs waiting for dependencies
        self.jobs = {}  # Store all Job objects by job_id
        self._keys = [] # Store all job names

        # Start the monitor thread which will check the status of all jobs
        self.monitor_thread = threading.Thread(target=self._monitor_all_jobs, daemon=True)
        self.monitor_thread.start()

    def execute_query(self, query, params=None):
        """Utility method to execute a query with optional parameters."""
        try:
            # Create job entry in the database
            # Create a cursor 
            cursor = self.conn.cursor()
            cursor.execute(query, params or [])
            self.conn.commit()
            return cursor
        except sqlite3.Error as e:
            nodus.__logger__.error(f"Database error: {e}")
            raise

    def _create_job_entry(self, name, parent_caller, job_type, nodus_session_id, command = None, script_path = None, 
                          status='pending', log_path=None, pid=None, config=None, **kwargs):
        
        """Create a new job in the database and handle job execution."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Make sure pid is text
        pid = f"{pid}" if pid else None

        # Create job entry in the database
        cursor = self.execute_query('''
            INSERT INTO jobs (nodus_session_id, parent_caller, job_name, status, timestamp, log_path, pid, config)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',(nodus_session_id, parent_caller, name, status, timestamp, log_path, pid, config))

        job_id = cursor.lastrowid

        # Log 
        nodus.__logger__.info(f"Job {name} with ID {job_id} created with status {status}.")

        return job_id

    def _add_job_dependency(self, job: List[Job | CommandJob | ScriptJob | AdoptedPIDJob],
                                  dependency_job : List[Job | CommandJob | ScriptJob | AdoptedPIDJob]):

        # Get job_id 
        job_id = job.job_id

        # Get dependant job_id
        dependency_job_id = dependency_job.job_id

        # Create job entry in the database
        query = "INSERT INTO job_dependencies (job_id, dependency_job_id) VALUES (?, ?)"
        cursor = self.execute_query(query, (job_id, dependency_job_id))
        
        # Add to job dependencies inside job object (dependencies is a set)
        job.dependencies.add(dependency_job_id)
        
        # Log
        #nodus.__logger__.info(f"Job {job.name} [{job_id}] depends on Job {dependency_job_id}.")
    
    def _check_circular_dependencies(self, job_id, dependencies, conn):
        """Ensure no circular dependencies exist."""
        def _has_circular_dependency(current_job, visited_jobs):
            if current_job in visited_jobs:
                return True
            visited_jobs.add(current_job)
            cursor = conn.cursor()
            query = "SELECT dependency_job_id FROM job_dependencies WHERE job_id = ?"
            cursor.execute(query, (current_job,))
            dependent_jobs = [row[0] for row in cursor.fetchall()]
            for dep_job in dependent_jobs:
                if _has_circular_dependency(dep_job, visited_jobs):
                    return True
            visited_jobs.remove(current_job)
            return False

        for dep in dependencies:
            if _has_circular_dependency(dep, set()):
                raise ValueError(f"Circular dependency detected for Job {job_id} and Dependency {dep}.")

    def create_job(self, parent_caller, job_type, name = None, nodus_session_id = None, dependencies = None, **kwargs):
        """
        Create a job with optional dependencies.

        Args:
            dependencies: List of job IDs this job depends on.
        """
        # Check name 
        if name is None:
            name = nodus.utils.get_next_name(self.name.replace('_job_manager','_job'), self._keys)
        if nodus_session_id is None:
            nodus_session_id = self.nodus_session_id

        # Create job entry in the database
        job_id = self._create_job_entry(name, parent_caller, job_type, nodus_session_id, 
                                        status='waiting' if dependencies else 'running', 
                                        **kwargs)
        
        # Validate circular dependencies
        if dependencies:
            self._check_circular_dependencies(job_id=job_id, dependencies=dependencies, conn=self.conn)

        # Create the Job object
        job_class = {'command': nodus.job.CommandJob, 'script': nodus.job.ScriptJob, 'pid': nodus.job.AdoptedPIDJob}.get(job_type, nodus.job.Job)
        job = job_class(name, job_id, nodus_session_id, **kwargs)

        # Update log_path 
        if job.log_path is not None:
            kwargs['log_path'] = job.log_path
            # Update 
            self.update_log_path(job, job.log_path)
        
        # If no dependencies, run the job immediately
        if not dependencies:
            job.run() # This updates job.pid
        else:
            # Add dependencies
            for dep_id in dependencies:
                # Find the dependent job object 
                dep_job = self.jobs.get(dep_id)
                if dep_job is None:
                    nodus.__logger__.error(f"Dependency Job {dep_id} not found.")
                    continue
                # Add the dependency
                self._add_job_dependency(job, dep_job)
        
            # Store in a waiting list 
            self.pending_jobs[job_id] = job
            # Log
            nodus.__logger__.info(f"Job {job.name} [{job_id}] waiting for dependencies to be resolved.")

        # Track the running PID and store the Job object
        if job.pid:
            self.running_pids[job.pid] = job_id
            self.execute_query('''
                               UPDATE jobs
                SET pid = ?
                WHERE job_id = ?
            ''', (str(job.pid), job_id))

            # Log 
            nodus.__logger__.info(f"Job {job_id} started with PID: {job.pid}")

        self.jobs[job_id] = job
        self._keys.append(name)
        
        return job_id, job

    def update_job_status(self, job_id, status, job_pid, completion_time=None, db_conn = None):
        """Update the status of a job."""
        if status in ['completed']:
            completion_time = completion_time or datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if db_conn is None:
            nodus.__logger__.info("No db connection, picking from self")
            cursor = self.conn
        else:
            cursor = db_conn

        if status == 'completed':
            job_pid_indicator = f"{job_pid}*" if job_pid else None
        else:
            job_pid_indicator = f"{job_pid}" if job_pid else None
        
        cursor = db_conn.cursor()
        cursor.execute('''
            UPDATE jobs
            SET status = ?, completion_time = ?, pid = ?
            WHERE job_id = ?
        ''', (status, completion_time, job_pid_indicator, job_id))
        db_conn.commit()

        # Log status updated 
        nodus.__logger__.info(f"Job {job_id} status updated to {status} and PID to {job_pid_indicator}.")

    def update_pid(self, job_id, job_pid, db_conn = None):
        
        if db_conn is None:
            nodus.__logger__.info("No db connection, picking from self")
            cursor = self.conn
        else:
            cursor = db_conn

        cursor = db_conn.cursor()
        cursor.execute('''
            UPDATE jobs
            SET pid = ?
            WHERE job_id = ?
        ''', (job_pid, job_id))
        db_conn.commit()

        # Log status updated 
        #nodus.__logger__.info(f"Job {job_id} pid as updated to {job_pid}.")


    def update_log_path(self, job, log_path):
        """Update the log path of a job."""
        job_id = job.job_id
        cursor = self.execute_query('''
            UPDATE jobs
            SET log_path = ?
            WHERE job_id = ?
        ''', (log_path, job_id))
        # Log 
        nodus.__logger__.info(f"Job {job.name} [{job_id}] log path updated to {log_path}")

    def delete_job(self, job_id):
        """Delete a job from the database."""
        cursor = self.execute_query('''
            DELETE FROM jobs
            WHERE job_id = ?
        ''', (job_id,))
        # Log job deleted
        nodus.__logger__.info(f"Job {job_id} deleted.")
    
    def kill_job(self, job_id):
        
        """Kill a running job."""
        job = self.jobs.get(job_id)
        if job is None:
            nodus.__logger__.error(f"Job {job.name} [{job_id}] not found.")
            return False

        if job.pid is None:
            nodus.__logger__.error(f"Job {job.name} [{job_id}] has no PID.")
            return False

        # If pid has the * at the end, skip cause this is an old PID and the process already ended
        if job.pid.endswith("*"):
            nodus.__logger__.error(f"Job {job.name} [{job_id}] with PID {job.pid} already ended.")
            return False

        # Check if the process is running
        if not nodus.utils.is_pid_running(job.pid):
            nodus.__logger__.error(f"Job {job.name} [{job_id}] with PID {job.pid} is not running.")
            # Update table to add the * at the end of the PID 
            cursor = self.execute_query('''
                UPDATE jobs
                SET pid = ?
                WHERE job_id = ?
            ''', (f"{job.pid}*", job_id))
            return False

        # Kill the process
        try:
            os.kill(job.pid, 9)
            nodus.__logger__.info(f"Job {job.name} [{job_id}] killed.")
            return True
        except Exception as e:
            nodus.__logger__.error(f"Failed to kill job {job.name} [{job_id}]: {e}")
            return False

    # get a list of jobs 
    def get_jobs(self):
        """Get a list of all jobs."""
        cursor = self.execute_query('''
            SELECT job_id, nodus_session_id, parent_caller, job_name, status, timestamp, completion_time, log_path, pid, config
            FROM jobs
            ORDER BY job_id DESC
        ''')
        jobs = cursor.fetchall()

        return jobs

    def get_job(self, job_id):
        """Get a specific job by its ID."""
        cursor = self.execute_query('''
            SELECT job_id, nodus_session_id, parent_caller, job_name, status, timestamp, completion_time, log_path, pid, config
            FROM jobs
            WHERE job_id = ?
        ''', (job_id,))
        job = cursor.fetchone()
        # Transform to dict
        job = dict(zip(['job_id', 'nodus_session_id', 'parent_caller', 'job_name', 'status', 'timestamp', 'completion_time', 'log_path', 'pid', 'config'], job))
        return job

    def wait_for_job_completion(self, job_id):
        """Wait for a job to complete."""
        while True:
            job = self.get_job(job_id)
            if job['status'] in ['completed', 'errored']:
                return job
            time.sleep(0.5)
    
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
            # To avoid racing conditions we will start by looking the jobs that are currently running and check
            # if they finished
            finished_pids = []
            finished_ids = []
            # Loop thru running jobs 
            for job_id, job in self.jobs.items():

                if job.status in ['completed', 'errored']:
                    continue # Skip already completed jobs

                # check the status of the job
                current_status = job._check_job_status()

                # current_status can be one of four: ['running', 'completed', 'errored', 'waiting']
                if current_status in ['completed', 'errored']:
                    # Update the job status to completed
                    self.update_job_status(job_id, current_status, job.pid, db_conn = conn)
                    # Remove the PID from the running list
                    finished_pids.append(job.pid)
                    finished_ids.append(job_id)

            # Remove finished jobs from the running list
            for job_pid, job_id in zip(finished_pids, finished_ids):
                if job_pid in self.running_pids:
                    del self.running_pids[job_pid]
            
            # Now that we have checked the running jobs, let's check the pending jobs. 
            # Here we will go thru the pending jobs and check if the status of ALL dependencies
            # is complete. Even if ONE status is errored, this job will also be errored and not run.
            remove_from_pending = []
            for job_id, job in self.pending_jobs.items():
                # Check for dependent jobs
                job_dependencies = job.dependencies
                # Loop thru dependencies and check if all are completed
                all_completed = True
                one_errored = False
                for dep_id in job_dependencies:
                    dep_job = self.jobs.get(dep_id)
                    if dep_job is None:
                        nodus.__logger__.error(f"Dependency Job {dep_id} not found.")
                        all_completed = False
                        one_errored = True
                        break
                    if dep_job.status != 'completed':
                        all_completed = False
                        one_errored |= dep_job.status == 'errored'
                        break
                
                # Check if all completed
                if all_completed:
                    # Now run 
                    job.run()
                    # Remove from pending
                    remove_from_pending.append(job_id)

                    # Track the running PID and store the Job object
                    if job.pid:
                        self.running_pids[job.pid] = job_id

                        # Update the table with the correct PID and new status 
                        self.update_pid(job_id, job.pid, db_conn = conn)
                        self.update_job_status(job_id, job.status, job.pid, db_conn = conn)
                    
                        # Log 
                        nodus.__logger__.info(f"Dependencies resolved for Job {job.name} [{job_id}]. Starting with PID: {job.pid}")
                elif one_errored:
                    # Update the job status to errored
                    self.update_job_status(job_id, 'errored', None, db_conn = conn)
                    # Remove from pending list
                    remove_from_pending.append(job_id)
                # else:
                #     nodus.__logger__.info(f"Dependencies not resolved for Job {job.name} [{job_id}]. Waiting...")

            # Remove from pending list
            for job_id in remove_from_pending:
                del self.pending_jobs[job_id]

            time.sleep(0.5)


