from src.CSVDataTable import CSVDataTable, DataTableException
import logging
import os
import time
import json


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.ERROR)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.ERROR)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../../Data/ClassicModels")


def test_load_fail():

    print("\n\n")
    print("******************** " + "test_load_fail" + " ********************")
    connect_info = {<!-- -->
        "directory": data_dir,
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    try:
        print("Table is orderdetails and key_columns are ['orderNumber']")
        csv_tbl = CSVDataTable("orderdetails", connect_info, key_columns=['orderNumber'])

        print("Loaded table = \n", csv_tbl)
        print("This is the wrong answer")

    except DataTableException as de:
        print("Load failed. Exception = ", de)
        print("This is the correct answer.")

    print("******************** " + "end test_load_fail" + " ********************")


def test_load_good():

    print("\n\n")
    print("******************** " + "test_load_good" + " ********************")

    connect_info = {<!-- -->
        "directory": data_dir,
        "file_name": "orderdetails.csv",
        "delimiter": ";"
    }

    try:
        csv_tbl = CSVDataTable("orderdetails", connect_info,
                               key_columns=['orderNumber', "orderLineNumber"])

        print("Loaded table = \n", csv_tbl)
        print("This is the correct answe")

    except DataTableException as de:
        print("Load failed. Exception = ", de)
        print("This is the wrong answer.")

    print("******************** " + "end test_load_ggod" + " ********************")


test_load_fail()
test_load_good()