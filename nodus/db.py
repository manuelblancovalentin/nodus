# nodus - Job Management Framework
# Author: Manuel Blanco Valentin
# Email: manuel.blanco.valentin@gmail.com
# Created: 2024-12-25
#
# Description:
# This file contains the Nodus class for managing the database of jobs.
# It provides methods for creating, inserting, updating, and querying job information.
#
# File: database.py
# Purpose:
# - Contains the Nodus class for interacting with the SQLite database
#   used to store and track job data, including job configurations,
#   statuses, and metadata.

""" Basic imports """
import os

# UUID 
import uuid

# Time management
from datetime import datetime

""" Database imports """
import sqlite3

# Import nodus itself
import nodus
from .manager import JobManager
from . import utils

""" Definition of the nodus database schemes """
# Jobs table
__JOBS_TABLE_SCHEME__ = '''
            CREATE TABLE IF NOT EXISTS jobs (
                job_id INTEGER PRIMARY KEY AUTOINCREMENT,
                nodus_session_id TEXT NOT NULL,
                parent_caller TEXT NOT NULL,
                job_name TEXT,
                status TEXT DEFAULT 'waiting',  -- 'waiting', 'running', 'complete', 'failed'
                timestamp TEXT NOT NULL,
                completion_time TEXT,
                log_path TEXT,
                pid TEXT DEFAULT NULL,
                config TEXT,
                command TEXT,
                priority INTEGER DEFAULT 0,
                script_path TEXT DEFAULT NULL
            )
        '''

# Jobs dependencies 
__DEPENDENCIES_TABLE_SCHEME__ = '''
            CREATE TABLE IF NOT EXISTS job_dependencies (
                job_id INTEGER,
                dependency_job_id INTEGER,
                FOREIGN KEY (job_id) REFERENCES jobs (job_id),
                FOREIGN KEY (dependency_job_id) REFERENCES jobs (job_id),
                PRIMARY KEY (job_id, dependency_job_id)
            );
        '''


""" 
    Nodus Session class (holds dbs and manages them)
"""
class NodusSession:
    def __init__(self):
        # Generate a unique session ID
        self.session_id = str(uuid.uuid4())

        """Initialize a new Nodus session."""
        self.start_time = datetime.now()
        self.log_file = nodus.__nodus_log_file__
        self.nodus_dbs = {}
        self._keys = []

    # Get item 
    def __getitem__(self, key):
        if isinstance(key, str):
            if key in self.nodus_dbs:
                return self.nodus_dbs[key]
            else:
                # Log 
                nodus.__logger__.error(f"No NodusDB instance with name '{key}' found.")
                raise KeyError(f"No NodusDB instance with name '{key}' found.")
        elif isinstance(key, int):
            if key < len(self._keys):
                return self.nodus_dbs[self._keys[key]]
            else:
                # Log
                nodus.__logger__.error(f"Index {key} out of range.")
                raise IndexError(f"Index {key} out of range.")

    def add_nodus_db(self, name = None, db_path=None, create_table=True):
        # If name is none, get next available name (nodus_db_1, nodus_db_2, ...)
        if name is None:
            name = utils.get_next_name('nodus_db', self._keys)
        """Add a new NodusDB instance to the session."""
        nodus_db = NodusDB(name, self.session_id, db_path = db_path, create_table = create_table) # This also initializes the db table
        # Add to the list of NodusDB instances
        self.nodus_dbs[name] = nodus_db
        self._keys.append(name)

        # Log 
        nodus.__logger__.info(f"Added NodusDB instance '{name}' linked to database '{nodus_db.db_path}'")

        return nodus_db

    def close(self):
        """Close all NodusDB connections in the session."""
        for nodus_db in self.nodus_dbs.values():
            nodus_db.close()
        
    # Representation 
    def __repr__(self):
        s = f"NodusSession (session_id: {self.session_id})\n"
        s += f"   - Start time: {self.start_time}\n"
        s += f"   - Log file: {self.log_file}\n"
        s += f"   - NodusDB instances:\n"
        if len(self.nodus_dbs) > 0:
            for name, nodus_db in self.nodus_dbs.items():
                s += f"       - {name}: {nodus_db.db_path}\n"
        else:
            s += "       - None\n"
        
        return s


class NodusDB:
    def __init__(self, name, session_id, db_path=None, create_table = True):
        """Initialize the NodusDB database."""
        self.name = name
        # Set the parent session ID
        self.session_id = session_id

        self.db_path = db_path or nodus.__nodus_db_path__
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        # Create the jobs table if it doesn't exist and check if each column exists
        if create_table: 
            self.create_jobs_table()
            self.create_job_dependencies_table()
        
        self.check_table()

        # Initialize the JobManager
        jm_name = f"{name}_job_manager"
        self.job_manager = JobManager(jm_name, self.db_path, self.session_id)

    def check_table(self):
        self.cursor.execute("PRAGMA table_info(jobs);")
        columns = [col[1] for col in self.cursor.fetchall()]

        # Compatibility check (we added priority and command on Jan/17/2025)
        if 'priority' not in columns:
            self.cursor.execute("ALTER TABLE jobs ADD COLUMN priority INTEGER DEFAULT 0;")
            self.conn.commit()
            nodus.__logger__.info(f"Added 'priority' column to jobs table in NodusDB instance '{self.name}'")
        if 'command' not in columns:
            self.cursor.execute("ALTER TABLE jobs ADD COLUMN command TEXT;")
            self.conn.commit()
            nodus.__logger__.info(f"Added 'command' column to jobs table in NodusDB instance '{self.name}'")
        if 'script_path' not in columns:
            self.cursor.execute("ALTER TABLE jobs ADD COLUMN script_path TEXT DEFAULT NULL;")
            self.conn.commit()
            nodus.__logger__.info(f"Added 'script_path' column to jobs table in NodusDB instance '{self.name}'")

    def create_jobs_table(self):
        """Create the jobs table if it doesn't exist."""
        self.cursor.execute(__JOBS_TABLE_SCHEME__)
        self.conn.commit()
        # Log 
        nodus.__logger__.info(f"Created jobs table in NodusDB instance '{self.name}'")

    def create_job_dependencies_table(self):
        """Create the job_dependencies table if it doesn't exist."""
        self.cursor.execute(__DEPENDENCIES_TABLE_SCHEME__)
        self.conn.commit()
        nodus.__logger__.info(f"Created job_dependencies table in NodusDB instance '{self.name}'")

    def close(self):
        """Close the database connection."""
        self.conn.close()
        # Log 
        nodus.__logger__.info(f"Closed NodusDB instance '{self.name}'")
    
    def __repr__(self):
        s = f'<NodusDB {self.name} - {self.db_path}>'
        # Add JobManager info
        s += f'\n    - JobManager: {self.job_manager}'
        return s


# Example usage
if __name__ == "__main__":
    
    import random

    # Initialize NodusSession
    session = NodusSession()
    print(session)
    db = session.add_nodus_db()  # Add a default NodusDB instance
    print(db)

    # Get the JobManager from the db instance
    jm = db.job_manager

    # Get a random letter (A,B,C,..)
    parent_caller = 'project_' + random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
    # Random PID number with 5 digits
    pid = random.randint(10000, 99999)

    # Insert a new job with some config
    config = {"param1": 42, "param2": "value"}
    job_type = 'command'
    job_id, job = jm.create_job(parent_caller, job_type, command=None, script_path=None, pid=None)

    # Query jobs
    jobs = jm.get_jobs(parent_caller=parent_caller)
    print(jobs)

    # Retrieve job config
    #job_config = nodus.get_job_config(1)
    #print(job_config)

    # Close connection
    session.close()