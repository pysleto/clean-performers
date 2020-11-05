"""
This program builds the company SQLite database from Pitchbook's csv file.
"""

import os
import csv
import json
from importlib import resources
import configparser as cfp
# import ConfigParser, ExtendedInterpolation
from pathlib import Path

cfg = cfp.ConfigParser(interpolation=cfp.ExtendedInterpolation())

with resources.path('config', 'config.ini') as path:
    cfg.read(path)


def get_company_data(file_path):
    """
    This function gets the data from the csv file
    """
    with open(file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data = [row for row in csv_reader]
        return data


def main():
    print("Starting")

    # Get the company data into a dictionary structure
    with Path(cfg['pitchbook']['company_csv_file']) as csv_file_path:
        data = get_company_data(csv_file_path)
        company_data = data

    print("Finished")


if __name__ == "__main__":
    main()