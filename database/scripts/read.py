import csv


def get_data_from_csv(file_path):
    """
    This function gets the data from the csv file
    """
    with open(file_path) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        data = [row for row in csv_reader]
        return data