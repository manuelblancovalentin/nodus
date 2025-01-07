""" Basic modules """
import os 
import math
from datetime import datetime

""" Async io """
import asyncio

""" Sqlite3 """
import sqlite3

""" Import nodus """
import nodus

""" Rich and Textual for UI """
from rich.text import Text

from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical, Grid, ScrollableContainer
from textual.widgets import Header, Footer, Button, Static, DataTable
from textual.events import MouseEvent

""" Definitions """
__COLUMNS__ = [('ID','job_id',{'width':5}), 
               ('PID','pid',{'width':8}), 
               ('Nodus Session ID', 'nodus_session_id', {'width':40}),
               ('Parent Caller', 'parent_caller', {'width':30}), 
               ('Job Name', 'job_name', {'width':20}), 
               ('Status', 'status', {'width':15}), 
               ('Start Time', 'timestamp', {'width':20}), 
               ('End Time', 'completion_time', {'width':20}),
               ('Runtime', None, {'width':15}),
               ('Log','log_path', {'width':50})]

""" Job list widget """
class JobList(DataTable):
    def __init__(self, db_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # set params 
        self.db_path = db_path
        self.selected_row_index = None  # Store the index of the selected row
        self.jobs_dict = []

        # First column is status (icon)
        self.add_column(" ", width=3)
        for column in __COLUMNS__:
            self.add_column(column[0], **column[2])
        
        # Enables row selection
        self.cursor_type = "row"  
    
    async def on_mount(self):
        """Set up the UI and start the refresh process."""
        self.refresh_task = asyncio.create_task(self.refresh_table())

    def get_status_icon(self, status):
        """Return a colored light icon based on job status."""
        if status == "completed":
            return Text("●", style="#00ff00")  # Green light
        elif status == "running":
            return Text("●", style="#00ffff")  # Cyan light for active jobs
        elif status == "errored":
            return Text("●", style="#ff0000")  # Red light
        elif status in ("pending", "waiting"):
            # yellow color 
            return Text("●", style="#ffff00")  # Yellow light
        return Text("●", style="#1f1f1f")  # Default gray light

    async def on_row_selected(self, event):
        row_data = event.row_key
        # Handle selection (e.g., display details in another panel)
        print(f"Selected Job: {row_data}")
        self.selected_row_index = event.row_index

    async def refresh_table(self):
        """Update the table with the latest jobs."""
        while True:
            """Refresh the table and keep the selected row."""
            selected_row = None
            if self.cursor_row is not None:
                # Store the selected row index before clearing
                selected_row = self.cursor_row
            
            jobs = await self.fetch_jobs()
            self.populate(jobs)

            if selected_row is not None:
                # Re-select the previously selected row after updating the table
                self.selected_row_index = selected_row
                self.move_cursor(row=selected_row)
                #self.select_row(self.selected_row_index)

            await asyncio.sleep(2)

    def populate(self, jobs):
        self.clear()  # Clear existing rows
        for job in jobs:
            icon = self.get_status_icon(job[5])  # Get status icon
            self.add_row(icon, *job)

    def cleanup_job(self, job):
        # First get vars 
        completion_time = job['End Time']
        completion_time = completion_time if completion_time else "N/A"
        pid = job['PID']
        parent_caller = job['Parent Caller']
        job_name = job['Job Name']
        timestamp = job['Start Time']
        log_path = job['Log']

        # Now compute runtime
        runtime = "N/A"
        if completion_time != "N/A" and timestamp:
            runtime = nodus.utils.compute_runtime(timestamp, completion_time)

        # Now update the job
        job['Runtime'] = runtime

        # If completion_time is None, display "N/A"
        job['End Time'] = completion_time
        # If PID is None, display "N/A"
        job['PID'] = pid if pid else "N/A"
        # If parent_caller is None, display "N/A"
        parent_caller = parent_caller if parent_caller else "N/A"
        # If parent_caller is too long, truncate it
        job['Parent Caller'] = parent_caller[:27] + "..." if len(parent_caller) > 30 else parent_caller
        # If job_name is None, display "N/A"
        job_name = job_name if job_name else "N/A"
        # If job_name is too long, truncate it
        job['Job Name'] = job_name #[:17] + "..." if len(job_name) > 20 else job_name

        # If log_path is None, N/A
        original_log_path = f'{log_path}'
        log_path = log_path if log_path else "N/A"
        # If log_path is too long, truncate it
        if len(log_path) > 50:
            lp = log_path.split(os.sep)[-1]
            log_path = f'…{os.sep}{lp[:25]}…{lp[-24:]}' if len(lp) > 50 else f"…{os.sep}{lp}"
        job['Log'] = log_path

        return job, original_log_path

    async def fetch_jobs(self):
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        fields = ', '.join([f[1] for f in __COLUMNS__ if f[1] is not None])
        cursor.execute(f"SELECT {fields} FROM jobs")
        jobs = cursor.fetchall()
        connection.close()

        # convert jobs to dict 
        jobs = [dict(zip([f[0] for f in __COLUMNS__ if f[1] is not None], job)) for job in jobs]

        # Now compute runtime
        jobs_out = []
        jobs_dict = []
        for job in jobs:
            # clean up 
            job, opath = self.cleanup_job(job)
            jobs_dict.append({**{kw: job[kw] for kw in job if kw != 'Log'}, 'Log': opath})
            # Transform back into tuple
            _j = ()
            for column in __COLUMNS__:
                _j += (job[column[0]],)
            jobs_out.append(_j)
        # Add jobs_dict to self for future use 
        self.jobs_dict = jobs_dict
        return jobs_out

    def delete(self, row_index):
        # Get row 
        job = self.jobs_dict.pop(row_index)
        job_id = job['ID']
        row = list(self.rows.items())[row_index][0]
        # Now remove from database 
        connection = sqlite3.connect(self.db_path)
        cursor = connection.cursor()
        cursor.execute(f"DELETE FROM jobs WHERE job_id = {job_id}")
        connection.commit()
        connection.close()
        self.remove_row(row)
    


class JobDetails(Static):
    """A widget to display details of a selected job."""
    def update_details(self, job):
        """Update the displayed job details."""
        if job:
            self.update(
                f"Job ID: {job['ID']}\n"
                f"Name: {job['Job Name']}\n"
                f"PID: {job['PID']}\n"
                f"Status: {job['Status']}\n"
                f"Start Time: {job['Start Time']}\n"
                f"End Time: {job['End Time']}\n"
                f"Runtime: {job['Runtime']}\n"
                f"Parent Caller: {job['Parent Caller']}\n"
                f"Nodus Session ID: {job['Nodus Session ID']}\n"
                f"Log: {job['Log']}"
            )

            #  [('ID','job_id',{'width':5}), 
            #    ('PID','pid',{'width':8}), 
            #    ('Nodus Session ID', 'nodus_session_id', {'width':40}),
            #    ('Parent Caller', 'parent_caller', {'width':30}), 
            #    ('Job Name', 'job_name', {'width':20}), 
            #    ('Status', 'status', {'width':15}), 
            #    ('Start Time', 'timestamp', {'width':20}), 
            #    ('End Time', 'completion_time', {'width':20}),
            #    ('Runtime', None, {'width':15}),
            #    ('Log','log_path', {'width':50})]
        else:
            self.update("No job selected.")

class JobLog(Static):

    """A widget to display the log of a selected job."""
    def compose(self) -> ComposeResult:
        with Vertical() as vertical:
            vertical.styles.padding = 1

            # Log title
            self.log_title = Static("No log selected.")
            self.log_title.styles.background = "purple"
            self.log_title.styles.margin = 0
            yield self.log_title

            # Scrollable container for the log content
            with ScrollableContainer() as scroll_view:
                scroll_view.border_title = "Log Content"
                scroll_view.styles.border = ('solid', 'black')
                scroll_view.styles.border_title_align = 'left'
                scroll_view.styles.background = "#F0F0D7"
                scroll_view.styles.color = "black"
                self.log_scroll_view = scroll_view

                # The log content widget (inside the scrollable area)
                self.log_content = Static("")
                self.log_content.styles.padding = 0
                yield self.log_content


    def update_details(self, log):
        """Update the displayed job log."""
        log_content = ""
        if log:
            if not os.path.exists(log):
                self.log_title.update(f"Log not found: {log}")
                return
            self.log_title.update(f"Log: {log}")
            # Read content of log 
            with open(log, 'r') as f:
                log_content = f.read()
            
            log_content = self.add_line_numbers(log_content)
            # self.log_line_numbers.update(lines)
            self.log_content.update(log_content)
        else:
            self.log_title.update("No log selected.")
        self.log_scroll_view.visible = (log_content != "")

    def add_line_numbers(self, text: str) -> str:
        """Prepend line numbers to each line in the log content."""
        lines = text.split("\n")

        nsp = math.log(len(lines)) + 1

        # find closest odd number 
        nsp = int(math.ceil(nsp) // 2 * 2 + 1) + 1

        numbered_lines = ["[black on #AAB99A]{}[/][black on #F0F0D7]{}[/]".format(str(i).center(nsp), line) for i, line in enumerate(lines)]
        return "\n".join(numbered_lines)

class NodusApp(App):
    TITLE = "Nodus Dashboard"
    BINDINGS = [
        ("q", "quit", "Quit"), #("r", "refresh", "Refresh"), #("d", "toggle_dark", "Toggle Dark Mode"),
        ("k", "kill", "Kill selected job entry"),
        ("escape", "unfocus_selected", "Unfocus selected job entry"),
        ("d", "delete_job_entry", "Delete selected job entry from db")
    ]
    CSS = """
    Vertical {
        padding: 0 ;
    }
    Container {
        padding: 0;  /* Dotted border for the main containers */
        width: 100%;
    }
    #job_list_group {
        height: 60%;
    }
    #job_bottom_group {
        height: 40%;
    }
    #job_details_group {
        height: 100%;
        width: 40%;
    }
    #job_log_group {
        height: 100%;
        width: 60%;
    }
    """

    def __init__(self, db_path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dark = False
        self.db_path = db_path

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()
        # Create the main layout with two groups: JobList and JobDetails
        with Vertical() as vertical:
            vertical.styles.padding = 0
            with Container(id="job_list_group") as c1:
                # Set container style
                c1.border_title=f"Job List from database @ {self.db_path}"
                c1.styles.border = ('round', 'white')
                c1.styles.border_title_align = 'left'
                c1.styles.padding = 1

                self.job_list = JobList(self.db_path)
                yield self.job_list

            with Horizontal(id = "job_bottom_group") as h:
                h.styles.padding = 0

                with Container(id="job_details_group") as c2:
                    # Set container style
                    c2.border_title="Job Details"
                    c2.styles.border = ('round', 'white')
                    c2.styles.border_title_align = 'left'
                    c2.styles.padding = 1

                    self.job_details = JobDetails("No job selected.")
                    yield self.job_details
                
                with Container(id="job_log_group") as c3:
                    # Set container style
                    c3.border_title="Job Log"
                    c3.styles.border = ('round', 'white')
                    c3.styles.border_title_align = 'left'
                    c3.styles.padding = 0

                    self.job_log = JobLog()
                    yield self.job_log

    async def on_data_table_row_selected(self, event: DataTable.RowSelected):
        """Handle row selection and update job details."""
        row_index = event.cursor_row
        jobs = self.job_list.jobs_dict
        selected_job = jobs[row_index] if row_index < len(jobs) else None
        self.job_details.update_details(selected_job)
        # Update log view
        self.job_log.update_details(selected_job['Log'] if selected_job else None)


    def on_key(self, event):
        """Handle keyboard arrow keys for navigation."""
        if event.key in ("up", "down"):
            current_row = self.job_list.cursor_row or 0

            # Move the cursor
            new_row = current_row
            if event.key == "up" and current_row > 0:
                # self.job_list.move_cursor(row=current_row-1)
                new_row = current_row - 1
            elif event.key == "down" and current_row < len(self.job_list.jobs_dict) - 1:
                # self.job_list.move_cursor(row=current_row+1)
                new_row = current_row + 1

            selected_job = self.job_list.jobs_dict[new_row] if new_row < len(self.job_list.jobs_dict) else None
            # Update job details
            self.job_details.update_details(selected_job)
            # Update log view
            self.job_log.update_details(selected_job['Log'] if selected_job else None)

    async def on_mount(self):
        jobs = await self.job_list.fetch_jobs()
        self.job_list.populate(jobs)

        """Set up the UI and start the refresh process."""
        self.refresh_task = asyncio.create_task(self.refresh_title())

    async def refresh_title(self):
        """Update the window title with the last update time."""
        while True:
            last_update_time = datetime.now()
            current_time = last_update_time.strftime('%H:%M:%S')
            self.title = f"Nodus Job Manager (Standalone Mode) - {current_time} (Updating every 2s)"
            await asyncio.sleep(2)

    def action_quit(self) -> None:
        self.exit()

    # def action_toggle_dark(self) -> None:
    #     self.dark = not self.dark

    def action_delete_job_entry(self):
        """Custom action to delete the selected job."""
        current_row = self.job_list.cursor_row
        if current_row is not None:
            self.job_list.delete(current_row)
            # Make sure we set the cursor to the next row
            if current_row < len(self.job_list.jobs_dict):
                self.job_list.move_cursor(row=current_row)
                # Update job details
                selected_job = self.job_list.jobs_dict[current_row] if current_row < len(self.job_list.jobs_dict) else None
                self.job_details.update_details(selected_job)
                # Update log view
                self.job_log.update_details(selected_job['Log'] if selected_job else None)
            
            else:
                self.job_details.update_details(None)
                self.job_log.update_details(None)
            
    def action_unfocus_selected(self):
        # TODO: THIS DOESN'T WORK
        self.job_list.move_cursor(row=None)
        self.job_list.selected_row_index = -1        
        self.job_details.update_details(None)
        self.job_log.update_details(None)

def run_ui():
    app = NodusApp(nodus.__nodus_db_path__)
    app.run()