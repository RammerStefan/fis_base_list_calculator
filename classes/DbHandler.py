# Class to handle all the Database stuff, happening on the single Databases
# Author: Stefan Rammer - Ski Austria Technology
# Mail: stefan.rammer@oesv.at

import warnings
import time

class DbHandler:
    def __init__(self, db):
        self.db = db
        self.cursor = db.cursor()
        self.cursor.execute("select database()")
        self.db_name = self.cursor.fetchone()[0]
        #self.cursor.execute("set global max_allowed_packet=67108864")

    #Simple Method to return the DB-Version of the Connection Object stored in self.db
    #--most likely used to check if Connection is working
    def getVersion(self):
        self.cursor.execute("SELECT VERSION()")
        data = self.cursor.fetchone()
        return (f'Database Vesion is: {data[0]}')

    #Simple Method to return the DB Name of the connection object stored in self.db
    def getDBName(self):
        return self.db_name

    #Returns an Object of type list containing all the table names in a database
    def getTableNames(self):
        self.cursor.execute(f"""SELECT * FROM INFORMATION_SCHEMA.TABLES
                            WHERE table_schema ='{self.db_name}'""")
        rawdata = self.cursor.fetchall()
        data = []
        for single in rawdata:
            data.append(single[2])
        return data

    #Method to return all the column data of a table
    def getColumnsInfo(self, tableName):
        self.cursor.execute(f"""SELECT * FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_NAME = '{tableName}'""")
        rawdata = self.cursor.fetchall()
        return rawdata

    #Method to return all the column-names of a table
    def getColumnNames(self, tableName):
        rawdata = self.getColumnsInfo(tableName)
        data = []
        for single in rawdata:
            data.append(single[3])
        return data

    #Method to return the number of rows in a table
    def getTableRowCount(self, tableName):
        self.cursor.execute(f"SELECT COUNT(*) FROM {tableName}")
        value = self.cursor.fetchone()
        return value[0]

    #Method to get all the table data in chungs of "size" default size is 100.000
    def getTableData(self, tableName, i, size=100000):
        self.cursor.execute(f"SELECT * FROM {tableName} LIMIT {i}, {size}")
        rows = self.cursor.fetchall()
        return rows


    #Method to get the create statement of a TABLE
    def getCreateStatement(self, tableName):
        self.cursor.execute(f"SHOW CREATE TABLE {tableName};")
        data = self.cursor.fetchall()
        return data[0][1]

    def getColumnCount(self, tableName):
        self.cursor.execute(f"""SELECT Count(*) FROM INFORMATION_SCHEMA.COLUMNS
                            WHERE table_schema = '{self.db_name}'
                            AND table_name = '{tableName}'""")
        value = self.cursor.fetchone()
        return value[0]

    #Method to get the column name of the primary key of a table
    def getPrimaryKeyName(self, tableName):
        self.cursor.execute(f"""SELECT KU.table_name as TABLENAME,column_name as PRIMARYKEYCOLUMN
                            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
                            INNER JOIN
                                INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
                                    ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
                                        TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
                                        KU.table_name='{tableName}'
                            ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;""")
        value = self.cursor.fetchone()
        #print(value)
        if value:
            return value[1]
        else:
            return []

    #Method to check if a table is allready in the database
    def checkTableName(self, tableName):
        if (tableName in self.getTableNames()):
            return True
        else:
            return False

    #Method to create new Tables (supresses Warnings from sql create)
    def createTable(self, sql):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.cursor.execute(sql)

    #Method to insert multiple rows into table
    def insertRowsIntoTable(self, tableName, rows):
        nCol = self.getColumnCount(tableName)
        key = self.getPrimaryKeyName(tableName)
        i=1
        valuesString = ""
        while i<nCol:
            valuesString = valuesString + "%s, "
            i += 1
        valuesString = valuesString + "%s"
        if key != []:
            sql_insert = (f"""INSERT INTO {tableName} VALUES ({valuesString}) ON DUPLICATE KEY UPDATE {key}={key}""")
            # print(sql_insert)
        else:
            sql_insert = (f"""INSERT INTO {tableName} VALUES ({valuesString})""")
            # print(sql_insert)
        self.cursor.executemany(sql_insert, rows)

    def dropTable(self, tableName):
        self.cursor.execute(f"DROP TABLE {tableName};")
