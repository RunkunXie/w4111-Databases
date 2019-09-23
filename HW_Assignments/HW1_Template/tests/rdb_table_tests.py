# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.RDBDataTable import RDBDataTable
import logging
import os
import json
import time
import pymysql

# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../HW1_Template/Data/Baseball")


def test_init_2():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)


def test_template_to_where_clause():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    where_clause = r_dbt.template_to_where_clause(template={"playerID": "aasedo01", "birthYear": "1954"})

    print(where_clause)


def test_create_select():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    select = r_dbt.create_select(table_name="people",
                                 template={"playerID": "aasedo01", "birthYear": "1954"})

    print(select)


def test_find_by_template():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    result = r_dbt.find_by_template(template={"playerID": "aardsda01", "birthYear": "1981"},
                                    field_list=["playerID", "birthYear", "birthMonth"])

    print(result)


def test_find_by_key():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    result = r_dbt.find_by_primary_key(key_fields=["aardsda01"],
                                       field_list=["playerID", "birthYear", "birthMonth"])

    print(result)


def test_create_delete():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    delete = r_dbt.create_delete(table_name="people",
                                 template={"playerID": "aasedo01", "birthYear": "1954"})

    print(delete)


def test_delete_by_template():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    result = r_dbt.delete_by_template(template={"playerID": "aasedo01", "birthYear": "1954"})

    print(result)


def test_delete_by_key():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    result = r_dbt.delete_by_key(key_fields=["aaronha01"])

    print(result)


def test_template_to_set_clause():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    set_clause = r_dbt.template_to_set_clause(template={"playerID": "aardsda01", "birthYear": "1981"})

    print(set_clause)


def test_create_update():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    update = r_dbt.create_update(table_name="people",
                                 template={"playerID": "aardsda01", "birthYear": "1981"},
                                 new_values={"birthYear": "3000"})

    print(update)


def test_update_by_template():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    print(r_dbt.find_by_template(template={"playerID": "aardsda01"}))

    result = r_dbt.update_by_template(template={"playerID": "aardsda01", "birthYear": "1981"},
                                      new_values={"birthYear": "3000"})

    print(result)
    print(r_dbt.find_by_template(template={"playerID": "aardsda01"}))


def test_update_by_key():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    print(r_dbt.find_by_template(template={"playerID": "aardsda01"}))

    result = r_dbt.update_by_key(key_fields=["aardsda01"],
                                 new_values={"birthYear": "4000"})

    print(result)
    print(r_dbt.find_by_template(template={"playerID": "aardsda01"}))


def test_insert():
    c_info = {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "dbuser",
        "password": "dbuserdbuser",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("people", connect_info=c_info, key_columns=['playerID'])

    r_dbt.insert(new_record={"playerID": "RX2166", "birthYear": "4000"})

    print(r_dbt.find_by_template(template={"playerID": "RX2166"}))


test_init_2()

test_template_to_where_clause()
test_create_select()
test_find_by_template()
test_find_by_key()

test_create_delete()
test_delete_by_template()
test_delete_by_key()

test_template_to_set_clause()
test_create_update()
test_update_by_template()
test_update_by_key()

test_insert()
