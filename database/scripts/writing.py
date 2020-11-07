import csv


def save_query_to_csv(path, query_data, fieldnames):
    """
    This function saves a query into a csv file
    """

    with open(path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, dialect='excel')

        writer.writeheader()
        for row in query_data:
            writer.writerow({key: value for (key, value) in row._asdict().items()})
