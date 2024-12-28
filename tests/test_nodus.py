# Import nodus 
import sys
sys.path.append('../nodus')

# Import args 
import argparse

# Import time
import time

# Import JobManager
import nodus
from nodus import JobManager


def run_script(script_path, shell = 'bash'):

    # Create Nodus Session
    session = nodus.NodusSession()
    db = session.add_nodus_db()  # Add a default NodusDB instance

    # Get the JobManager from the db instance
    jm = db.job_manager

    # Create a new job to run the script
    job_id = jm.create_job(
        name="example_job",
        parent_caller="example_script_runner",
        job_type="script",
        script_path = script_path,
        shell = shell
    )

    # Let the JobManager monitor jobs (in practice, your UI would run alongside this)
    while True:
        time.sleep(1)  # Keep the main thread alive


def run_command(command):
    # Create Nodus Session
    session = nodus.NodusSession()
    db = session.add_nodus_db()  # Add a default NodusDB instance

    # Get the JobManager from the db instance
    jm = db.job_manager

    # Create a new job to run the command
    job_id = jm.create_job(
        name="example_job",
        parent_caller="example_command_runner",
        job_type="command",
        command = command
    )

    # Let the JobManager monitor jobs (in practice, your UI would run alongside this)
    while True:
        time.sleep(1)  # Keep the main thread alive


def test_job_creation():
    # Initialize JobManager
    job_manager = JobManager()

    # Create a sample job
    job_id = job_manager.create_job(parent_caller='test', 
                                    status='pending', 
                                    log_path='/logs/test.log', 
                                    pid=None, 
                                    config='default_config')

    # Assert that the job ID is not None
    assert job_id is not None, "Job ID should not be None"

    # Wait for some time
    time.sleep(5)

    # Update job status to 'completed'
    job_manager.update_job_status(job_id, status='completed', completion_time=None)

    # Verify the job status in the database
    db_path = nodus.__nodus_db_path__
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM jobs WHERE job_id = ?", (job_id,))
    status = cursor.fetchone()[0]
    conn.close()

    assert status == 'completed', f"Expected job status to be 'completed', but got '{status}'"

    # Cleanup: Delete the test job
    job_manager.delete_job(job_id)


def test_run_command(command, shell='bash'):
    # Initialize JobManager
    job_manager = JobManager()

    # Create a sample job
    job_id = job_manager.create_job(parent_caller='test', 
                                    status='pending', 
                                    log_path='/logs/test.log', 
                                    pid=None, 
                                    config='default_config')

    # Assert that the job ID is not None
    assert job_id is not None, "Job ID should not be None"






if __name__ == "__main__":

    # Parse arguments
    parser = argparse.ArgumentParser(description='Test Nodus Job Manager')

    # Add arguments
    parser.add_argument('-t', '--test', type=str, help='Test to run')
    parser.add_argument('--script', type=str, help='Script to run')
    parser.add_argument('--command', type=str, help='Command to run')
    parser.add_argument('--shell', type=str, help='Shell to use (e.g., bash, zsh)')

    # Parse arguments
    args = parser.parse_args()

    # Get shell
    shell = args.shell
    test = args.test
    script_path = args.script
    command = args.command

    # Run the test
    if test == 'job_creation':
        test_job_creation()
    elif test == 'script':
        script_path = 'tests/example_script.sh' if script_path is None else script_path
        run_script(script_path, shell=shell)
    elif test == 'command':
        nodus_command = "bash -c 'for i in {1..10}; do echo Step $i; sleep 1; done'"
        command = nodus_command if command is None else command
        run_command(command)