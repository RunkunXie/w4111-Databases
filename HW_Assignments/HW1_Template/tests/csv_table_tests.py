# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
import logging
import os

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")
# data_dir = os.path.abspath("../HW1_Template/Data/Baseball")


def test_init():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))


def test_matches_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    match = csv_tbl.matches_template(csv_tbl.rows[0], template={"playerID": "aasedo01", "birthYear": "1954"})

    print(match)

    match = csv_tbl.matches_template(csv_tbl.rows[3], template={"playerID": "aasedo01", "birthYear": "1954"})

    print(match)


def test_get_fields():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    get_field = csv_tbl.get_fields(csv_tbl.rows[0], field_list=["playerID", "birthYear"])

    print(get_field)


def test_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    result1_single_record = csv_tbl.find_by_template(template={"playerID": "aasedo01", "birthYear": "1954"},
                                                     field_list=["playerID", "birthYear", "birthMonth"])

    print(result1_single_record)

    result2_multi_records = csv_tbl.find_by_template(template={"birthYear": "1954"},
                                                     field_list=["playerID", "birthYear", "birthMonth"])

    print(result2_multi_records)


def test_key_to_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    template = csv_tbl.key_to_template(key_fields=["aardsda01"])

    print(template)


def test_find_by_primary_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    result = csv_tbl.find_by_primary_key(key_fields=["aasedo01"],
                                         field_list=["playerID", "birthYear", "birthMonth"])

    print(result)


def test_delete_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    result1_delete_single = csv_tbl.delete_by_template(template={"playerID": "aasedo01", "birthYear": "1954"})
    result2_delete_multiple = csv_tbl.delete_by_template(template={"birthYear": "1954"})

    print(result1_delete_single)
    print(result2_delete_multiple)


def test_delete_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    result = csv_tbl.delete_by_key(key_fields=["aasedo01"])

    print(result)


def test_update_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    n = csv_tbl.update_by_template(template={"birthYear": "1954"}, new_values={"birthYear": "3000", "birthMonth": "1"})

    print(n, "recolds updated.")
    print(csv_tbl.find_by_template(template={"birthYear": "3000"}, field_list=["playerID", "birthYear", "birthMonth"]))


def test_update_by_key():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    n = csv_tbl.update_by_key(key_fields=["aasedo01"], new_values={"birthYear": "3000", "birthMonth": "1"})

    print(n, "recolds updated.")
    print(csv_tbl.find_by_template(template={"playerID": "aasedo01"}, field_list=["playerID", "birthYear", "birthMonth"]))


def test_insert():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, key_columns=["playerID"])

    csv_tbl.insert({"playerID": "rx2166", "birthYear": 1996})

    print(csv_tbl.find_by_primary_key(["rx2166"], ["playerID", "birthYear"]))


test_init()

test_matches_template()
test_get_fields()
test_find_by_template()

test_key_to_template()
test_find_by_primary_key()

test_delete_by_template()
test_delete_by_key()

test_update_by_template()
test_update_by_key()

test_insert()
