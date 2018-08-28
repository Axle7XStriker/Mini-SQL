#!/usr/bin/python3

from table import Table

class Database:
    """
        Database class for tracking and storing multiple related tables 
    """
    def __init__(self, name, metadata_file):
        self.__name = name
        self.__metadata_file = metadata_file
        self.__tables = {}
        self.__error_messages = {
            1146: "ERROR 1146 (42S02): Table doesn't exist",
            404: "ERROR 404 (1234): Data for table doesn't exist",
            500: "ERROR 500 (56789): Table couldn't be loaded"
        }

        self.load_metadata()
        self.load_tables()

    def load_metadata(self):
        with open(self.__metadata_file) as file:
            content = [x.strip() for x in file.readlines()]
            print(content)      # Check working 
            i = 0
            while i < len(content):
                if content[i] == '<begin_table>':
                    table = Table(content[i+1])
                    i += 2
                    field_idx = 1
                    while content[i] != '<end_table>':
                        table.add_field(content[i], 'INTEGER', field_idx)
                        field_idx += 1
                        i += 1
                    self.__tables[table.get_name()] = table
                i += 1
    
    def has_table(self, table):
        return table in self.__tables

    def get_table(self, table):
        return self.__tables[table]

    def load_tables(self):
        for table in self.__tables:
            #try:
                self.__tables[table].load()
            #except:
            #    print(self.__error_messages[500])
