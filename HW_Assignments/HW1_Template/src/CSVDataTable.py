from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)


class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    @staticmethod
    def get_fields(row, field_list):
        result = {}
        for f in field_list:
            # A good program would catch an exception if field is not in the row.
            # Do not worry about it. This is not a robust programming course.
            result[f] = row[f]

        return result

    def key_to_template(self, key_fields):
        template = {}
        for i, key in enumerate(self.data['key_columns']):
            template[key] = key_fields[i]

        return template

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        template = self.key_to_template(key_fields)
        result = self.find_by_template(template, field_list)

        return result

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        result = None
        for row in self.rows:
            if self.matches_template(row, template):
                if result is None:
                    result = []
                result.append(self.get_fields(row, field_list))

        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to delete a record.
        :return: A count of the rows deleted.
        """
        template = self.key_to_template(key_fields)
        result = self.delete_by_template(template)

        return result

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        n = len(self.rows)
        lines = self.rows
        for row in self.rows:
            if self.matches_template(row, template):
                lines.remove(row)
        self._rows = lines

        return n - len(self.rows)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        template = self.key_to_template(key_fields)
        result = self.update_by_template(template, new_values)

        return result

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        n = 0
        for i, row in enumerate(self._rows):
            if self.matches_template(row, template):
                n += 1
                for k, v in new_values.items():
                    self._rows[i][k] = v

        return n

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        self._rows.append(new_record)

    def get_rows(self):
        return self._rows

    def get_data(self):
        return self._data

    @property
    def rows(self):
        return self._rows

    @property
    def data(self):
        return self._data
