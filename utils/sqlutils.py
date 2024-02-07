import mysql.connector
from env import ConstantVariable
class SQL_Utils():
    def __init__(self, tableName, tableQuery) -> None:
        self.conn = mysql.connector.connect(
            host=ConstantVariable.MYSQL_HOST,
            user=ConstantVariable.MYSQL_USERNAME,
            password=ConstantVariable.MYSQL_PASSWORD,
            database=ConstantVariable.MYSQL_DATABASE
        )
        if self.conn.is_connected():
            print("Connected to MySQL server")
        else:
            print("Failed to connect to MySQL server")

        self.cursor = self.conn.cursor()

        create_table_query = tableQuery
        self.tableName = tableName
        self.cursor.execute(create_table_query)
        self.conn.commit()

        query = "DESCRIBE {table};".format(table=self.tableName)
        self.cursor.execute(query)
        self.fields = [column[0].upper() for column in self.cursor.fetchall()]
        self.conn.close()

    def reset_connect(self):
        '''
        Usage:
        ------
            - Reset the mysql database connection
        '''

        self.conn = mysql.connector.connect(
            host=ConstantVariable.MYSQL_HOST,
            user=ConstantVariable.MYSQL_USERNAME,
            password=ConstantVariable.MYSQL_PASSWORD,
            database=ConstantVariable.MYSQL_DATABASE
        )
        if self.conn.is_connected():
            print("Connected to MySQL server")
        else:
            print("Failed to connect to MySQL server")
        self.cursor = self.conn.cursor()

    def create_property(self, propertyName: str, propertyDataType: str) -> bool:
        '''
        Usage:
        ------
            - Create a new column for the table

        Parameters:
        -----------
            - propertyName (str): name for new column
            - propertyDataType (str): data type for new column

        Returns:
        -------
            - Whether the column have been added
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        try:
            query = "ALTER TABLE {TABLE} ADD COLUMN {COLUMN_NAME} {COLUMN_TYPE};".format(
                TABLE=self.tableName, COLUMN_NAME=propertyName, COLUMN_TYPE=propertyDataType)
            self.cursor.execute(query)
            self.conn.commit()
            self.conn.close()
            return True
        except mysql.connector.Error as e:
            print("MySQL Error:", e)
            self.conn.rollback()
            self.conn.close()
            return False

    def delete_property(self, propertyName: str) -> bool:
        '''
        Usage:
        ------
            - Delete a column in the table

        Parameters:
        -----------
            - propertyName (str): name of column

        Returns:
        -------
            - Whether the column have been deleted
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        try:
            self.cursor.execute('ALTER TABLE {TABLE} DROP COLUMN {COLUMN_NAME};'.format(
                TABLE=self.tableName, COLUMN_NAME=propertyName))
            self.conn.commit()
            self.conn.close()
            return True
        except mysql.connector.Error as e:
            print("MySQL Error:", e)
            self.conn.rollback()
            self.conn.close()
            return False

    def create_record(self, data: dict) -> bool:
        '''
        Usage:
        ------
            - Create a new record for the table

        Parameters:
        -----------
            - data (dict) (Ex: {'name': 'Adam', 'age': '12'})

        Returns:
        -------
            - Whether the record has created
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()


        if set(list(map(lambda key: key.upper(), data.keys()))).issubset(self.fields):
            fieldsQuery = ', '.join(list(data.keys()))
            valuesQuery = ', '.join("'" + str(value).replace("'", "_") + "'" for value in data.values())

            query = 'INSERT INTO {table} ({fields}) VALUES ({values});'.format(
                table=self.tableName, fields=fieldsQuery, values=valuesQuery)
            try:
                self.cursor.execute(query)
                self.conn.commit()
                self.conn.close()
                return True
            except mysql.connector.Error as e:
                print("MySQL Error:", e)
                self.conn.rollback()
                self.conn.close()
                return False
        else:
            print("Invalid Columns hence cannot create record")
            self.conn.close()
            return False

    def delete_record(self, condition=None) -> bool:
        '''
        Usage:
        ------
            - Delete the records

        Parameters:
        -----------
            - condition (str): The condition for the query (Ex: "username LIKE '%john%' AND age > 25;")

        Returns:
        --------
            - Whether the records have been deleted
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        try:
            if condition is None:
                query = 'DELETE FROM {table};'.format(table=self.tableName)
            else:
                query = 'DELETE FROM {table} WHERE {conditionsQuery};'.format(
                    table=self.tableName, conditionsQuery=condition)
            self.cursor.execute(query)
            self.conn.commit()
            self.conn.close()
            return True
        except mysql.connector.Error as e:
            print("MySQL Error:", e)
            self.conn.rollback()
            self.conn.close()
            return False

    def update_record(self, newValue: dict, condition=None) -> bool:
        '''
        Usage:
        ------
            - Update the records

        Parameters:
        -----------
            - newValue (dict): The new value for records (Ex: {'name': 'Adam', 'age': '12'})
            - condition (str): The condition for the query (Ex: "username LIKE '%john%' AND age > 25;")

        Returns:
        -------
            - Whether the records have been updated
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        if set(list(map(lambda key: key.upper(), newValue.keys()))).issubset(self.fields):
            newQuery = ', '.join("{field}='{value};'".format(
                field=field, value=newValue[field]) for field in list(newValue.keys()))

            query = 'UPDATE {table} SET {newValue} WHERE {conditionsQuery};'.format(
                table=self.tableName, newValue=newQuery, conditionsQuery=condition)
            try:
                self.cursor.execute(query)
                self.conn.commit()
                self.conn.close()
                return True
            except mysql.connector.Error as e:
                print("MySQL Error:", e)
                self.conn.rollback()
                self.conn.close()
                return False
        else:
            print("Invalid Columns hence cannot update record")
            self.conn.close()
            return False

    def read_record(self, condition=None) -> list:
        '''
        Usage:
        ------
            - Read records with the condition (or all records)

        Parameters:
        -----------
            - condition (str): The condition for the query (Ex: "username LIKE '%john%' AND age > 25;")

        Returns:
        -------
            - List of records satisfy the condition
            - Example: [{'name': 'ALEX', 'age': '12'}, {'name': 'ADAM', 'age': '23'}]
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        if condition is None:
            query = 'SELECT * FROM {table};'.format(table=self.tableName)
            self.cursor.execute(query)
            data = self.cursor.fetchall()
        else:
            query = 'SELECT * FROM {table} WHERE {conditionsQuery};'.format(table=self.tableName, conditionsQuery=condition)
            self.cursor.execute(query)
            data = self.cursor.fetchall()

        result = [dict(zip(self.fields, record)) for record in data]
        self.conn.close()
        return result

    def get_properties(self) -> list:
        '''
        Usage:
        ------
            - Get all the columns in table

        Returns:
        -------
            --> {'name', 'type', 'notnull', 'pk', 'dflt_value', 'extra'}
            - name: The name of the column.
            - type: The data type of the column.
            - notnull: Indicates whether the column can contain NULL values. 1 means NOT NULL, and 0 means NULL is allowed.
            - dflt_value: The default value of the column.
            - pk: Indicates whether the column is part of the primary key. 1 means the column is part of the primary key, and 0 means it's not.
            - extra: This field provides additional information about the column. Common values include:
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        self.cursor.execute("DESCRIBE {TABLE};".format(TABLE=self.tableName))
        columns_info = list(self.cursor.fetchall())
        field = ['name', 'type', 'notnull', 'pk', 'dflt_value', 'extra']
        list_columns = [dict(zip(field, column)) for column in columns_info]
        self.conn.close()
        return list_columns

    def get_unique_values(self, propertyName: str) -> list:
        '''
        Usage:
        ------
            - Get all unique value of a column

        Parameters:
        -----------
            - propertyName (str): name of column

        Returns:
        -------
            - List of unique value
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        self.cursor.execute(
            "SELECT DISTINCT {COLUMN} FROM {TABLE};".format(COLUMN=propertyName, TABLE=self.tableName))
        unique_values = self.cursor.fetchall()
        self.conn.close()
        return [_[0] for _ in unique_values]

    def get_enums(self, propertyName: str) -> list:
        '''
        Usage:
        ------
            - Get all enums of a column

        Parameters:
        -----------
            - propertyName (str): name of column

        Returns:
        -------
            - List of enums
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        try:
            query ="""
                SELECT SUBSTRING(COLUMN_TYPE, 6, LENGTH(COLUMN_TYPE) - 6)
                FROM information_schema.COLUMNS
                WHERE TABLE_NAME = '{TABLE}'
                AND COLUMN_NAME = '{COLUMN}';
                    """.format(TABLE=self.tableName, COLUMN=propertyName)

            self.cursor.execute(query)
            enum_values = self.cursor.fetchone()[0]
            enum_values_list = [value.strip("'") for value in enum_values.split(",")]

            self.conn.close()
            return enum_values_list

        except mysql.connector.Error as e:
            print(f"MySQL error: {e}")
            self.conn.close()
            return []

    def extend_enum(self, propertyName: str, extendValue: list) -> bool:
        '''
        Usage:
        ------
            - Extend enums value could be used for a column

        Parameters:
        -----------
            - propertyName (str): name of column
            - extendValue (list): list of new value (['cat', 'dog'])

        Returns:
        -------
            - Whether the enum have been added
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        try:
            newValue = "', '".join(self.get_enums(propertyName) + extendValue)
            self.reset_connect()
            alter_query = """ALTER TABLE {TABLE} MODIFY COLUMN {COLUMN} ENUM('{NEW_VALUE}') NOT NULL;
                        """.format(TABLE=self.tableName, COLUMN=propertyName, NEW_VALUE=newValue)

            self.cursor.execute(alter_query)
            self.conn.commit()
            self.conn.close()
            return True

        except mysql.connector.Error as e:
            print(f"MySQL error: {e}")
            self.conn.close()
            return False

    def remove_enum(self, propertyName: str, removeValue: list) -> bool:
        '''
        Usage:
        ------
            - Remove enums value could be used for a column

        Parameters:
        -----------
            - propertyName (str): name of column
            - extendValue (list): list of new value (['cat', 'dog'])

        Returns:
        -------
            - Whether the enum have been deleted
        '''

        if (self.conn.is_connected() == False):
            self.reset_connect()

        try:
            newValue = self.get_enums(propertyName)
            self.reset_connect()
            for _ in removeValue:
                newValue.remove(_)
            newValue = "', '".join(newValue)
            alter_query = """ALTER TABLE {TABLE} MODIFY COLUMN {COLUMN} ENUM('{NEW_VALUE}') NOT NULL
                        """.format(TABLE=self.tableName, COLUMN=propertyName, NEW_VALUE=newValue)

            self.cursor.execute(alter_query)
            self.conn.commit()
            self.conn.close()
            return True

        except mysql.connector.Error as e:
            print(f"MySQL error: {e}")
            self.conn.close()
            return False