from src.BaseDataTable import BaseDataTable
import pymysql
import src.dbutils as dbutils


class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        self._cnx = dbutils._get_default_cnx()

        # cnx = dbutils.get_connection(connect_info)
        # if cnx is not None:
        #     self._cnx = cnx
        # else:
        #     raise Exception("Could not get a connection.")

    @staticmethod
    def template_to_where_clause(template):
        """

        :param template: One of those weird templates
        :return: WHERE clause corresponding to the template.
        """

        if template is None or template == {}:
            result = (None, None)
        else:
            args = []
            terms = []

            for k, v in template.items():
                terms.append(" " + k + "=%s ")
                args.append(v)

            w_clause = "AND".join(terms)
            w_clause = " WHERE " + w_clause

            result = (w_clause, args)

        return result

    @staticmethod
    def template_to_set_clause(template):
        """

        :param template: One of those weird templates
        :return: WHERE clause corresponding to the template.
        """

        if template is None or template == {}:
            result = (None, None)
        else:
            args = []
            terms = []

            for k, v in template.items():
                terms.append(" " + k + "=%s ")
                args.append(v)

            w_clause = "AND".join(terms)
            s_clause = " SET " + w_clause

            result = (s_clause, args)

        return result

    def key_to_template(self, key_fields):
        template = {}
        for i, key in enumerate(self.data['key_columns']):
            template[key] = key_fields[i]

        return template

    def create_select(self, table_name, template, fields=None, order_by=None, limit=None, offset=None):
        """
        Produce a select statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param fields: Columns to select (an array of column name)
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :param order_by: Ignore for now.
        :param limit: Ignore for now.
        :param offset: Ignore for now.
        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        if fields is None:
            field_list = " * "
        else:
            field_list = " " + ",".join(fields) + " "

        w_clause, args = self.template_to_where_clause(template)

        sql = "SELECT " + field_list + " FROM " + table_name + " " + w_clause

        return sql, args

    def create_delete(self, table_name, template):
        """
        Produce a select statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.

        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        w_clause, args = self.template_to_where_clause(template)

        sql = "DELETE FROM " + table_name + " " + w_clause

        return sql, args

    def create_update(self, table_name, template, new_values):
        """
        Produce a select statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :param new_values:

        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        w_clause, w_args = self.template_to_where_clause(template)
        s_clause, s_args = self.template_to_set_clause(new_values)

        sql = "UPDATE " + table_name + s_clause + w_clause

        return sql, s_args+w_args

    @staticmethod
    def create_insert(table_name, new_record):
        """
        Produce a insert statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param new_record:

        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        new_values = ' (' + ','.join(['%s']*len(new_record.values())) + ') '
        field_list = ' (' + ','.join(new_record.keys()) + ') '

        sql = "INSERT INTO " + table_name + field_list + " VALUES " + new_values
        arg = list(new_record.values())

        return sql, arg

    def get_row_count(self):

        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = dbutils.run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0]['count']
            self._data['"row_count'] = row_count

        return row_count

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        template = self.key_to_template(key_fields)

        return self.find_by_template(template, field_list)

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

        where_clause = self.template_to_where_clause(template)
        select = self.create_select(self._data["table_name"], template=template, fields=field_list)
        result = dbutils.run_q(select[0], select[1], fetch=True, conn=self._cnx, commit=True)

        return result

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to delete a record.
        :param template: A template.
        :return: A count of the rows deleted.
        """
        template = self.key_to_template(key_fields)

        return self.delete_by_template(template)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        n_rows = self.get_row_count()

        delete = self.create_delete(self._data["table_name"], template=template)
        result = dbutils.run_q(delete[0], delete[1], fetch=True, conn=self._cnx, commit=True)

        return n_rows - self.get_row_count()

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        template = self.key_to_template(key_fields)

        return self.update_by_template(template, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        update = self.create_update(self._data["table_name"], template=template, new_values=new_values)
        result = dbutils.run_q(update[0], update[1], fetch=True, conn=self._cnx, commit=True)

        return result[0]

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        insert = self.create_insert(self._data["table_name"], new_record=new_record)
        dbutils.run_q(insert[0], insert[1], fetch=True, conn=self._cnx, commit=True)

    def get_data(self):
        return self._data

    @property
    def data(self):
        return self._data
