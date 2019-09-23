import csv
import os
import json


def load_csv_file(fn):
    # Open the file in read model
    with open(fn, "r") as in_file:
        # Wrap with class that helps reading CSV files.
        csv_rdr = csv.DictReader(in_file)

        # Place to hold the result
        result = []

        # Build the result
        for r in csv_rdr:
            result.append(r)

    return result


_default_directory = os.path.abspath("../HW1_Template/Data/Baseball/People.csv")
fn = os.path.join(_default_directory, _default_directory)

the_people = load_csv_file('r'+fn)

print("Load got", len(the_people), "rows.")

print("A typical row looks like: \n", json.dumps(the_people[0], indent=2))