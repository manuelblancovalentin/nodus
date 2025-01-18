# nodus/__main__.py
from .ui import run_ui  # Replace with the actual function you want to run

# Import argparse
import argparse

if __name__ == "__main__":

    # Parse db name 
    parser = argparse.ArgumentParser(description='Nodus UI')
    parser.add_argument('--db', type=str, help='Name of the database to use')

    args = parser.parse_args()

    db_name = None
    if args.db:
        db_name = args.db

    run_ui(db_name)  # Call the main function of your UI