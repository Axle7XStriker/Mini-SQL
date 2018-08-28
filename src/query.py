#!/usr/bin/python3

import sqlparse

class Query:
    """
        Query Class for parsing SQL queries 
    """
    def __init__(self, raw_query):
        self.__supported_types = ['SELECT']
        self.__error_messages = {
            0: "See you soon. Goodbye!",
            1064: "ERROR 1064 (42000): You have an error in your SQL syntax; check the manual for the right syntax to use.",
            404: "Error 404: You have used a DDL/DML type in your SQL syntax, which is currently not supported by the MiniSQL engine."
        }
        self.__raw_query = raw_query
        self.__parsed_query = ""

    def get_type(self):
        return self.__parsed_query.get_type()

    def get_parsed_query(self):
        return self.__parsed_query

    def type_verify(self, parsed_query):
        query_type = parsed_query.get_type()
        if query_type == u'UNKNOWN':
            first_token = str(parsed_query.tokens[0]).upper()
            if first_token == 'EXIT' or first_token == 'QUIT':
                print(self.__error_messages[0])
                exit(0)
            else :
                print(self.__error_messages[1064])
                return False
        return True

    def type_supported(self, query_type):
        if query_type not in self.__supported_types:
            print(self.__error_messages[404])
            return False
        return True

    def parse(self):
        parsed_query = sqlparse.parse(self.__raw_query)[0]
        if self.type_verify(parsed_query):
            query_type = parsed_query.get_type()
            if self.type_supported(query_type):
                self.__parsed_query = parsed_query
                return True
        return False
