<img src="imgs/logo.png" alt="drawing" style="width:200px;"/>

# Nodus: A lightweight and reusable job manager.

Lorem ipsum dolor sit amet, consectetur adipiscing elit. Suspendisse id molestie elit, semper auctor ex. Phasellus vestibulum, elit vitae maximus mollis, odio nunc laoreet magna, ac venenatis neque odio ac sem. Pellentesque vel mauris laoreet, semper nunc at, tempor felis. Morbi at magna dui. Sed a venenatis mauris, at auctor velit. Pellentesque luctus, eros sed sagittis molestie, orci ex elementum nisl, at scelerisque lorem felis vel felis. Ut eget elit pellentesque, egestas quam sed, luctus diam. Cras ex risus, convallis id enim eget, malesuada consequat mi.

## 1. How to install

```bash
pip install nodus
```

## 2. How to use 

```python
# Import wsbmr
import nodus
import time 

# Create Nodus Session
session = nodus.NodusSession()
db = session.add_nodus_db()  # Add a default NodusDB instance

# Get the JobManager from the db instance
jm = db.job_manager

# Initialize the command
nodus_command = "bash -c 'for i in {1..10}; do echo Step $i; sleep 1; done'"

# Create a new job to run the command
job_id = jm.create_job(
    name="example_job",
    parent_caller="example_command_runner",
    job_type="command",
    command = nodus_command
)

# Let the JobManager monitor jobs (in practice, your UI would run alongside this)
while True:
    time.sleep(1)  # Keep the main thread alive

# Then after a while
# Close connection
session.close()
```


Using the UI:

```bash
python -m nodus
```

![UI](imgs/ui.png)


## 3. Pypip

https://pypi.org/project/nodus/0.0.1/

<!--https://test.pypi.org/project/nodus/0.0.1/--> 