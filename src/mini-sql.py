#!/usr/bin/python3

import sys
import sqlparse

from database import Database
from query import Query
from select_query import Select

class Engine:
    """
        Class for getting, handling and executing SQL queries 
    """
    def __init__(self):
        self.__types = {
            'SELECT': Select(),
        }
        self.__db = Database("TEST_DB", "../sample-files/metadata.txt")
        self.__history = []
        self.__raw_input = ""
        self.__queries = []

    def execute(self, raw_input = ""):
        if len(raw_input):
            self.__raw_input = raw_input[0]
        self.__history.append(self.__raw_input)
        while True:
            # Get input from the user 
            if not self.__raw_input:
                self.__raw_input = input("MiniSQL=> ")
            
            # Split the raw query into a list of atomic queries 
            for raw_query in sqlparse.split(self.__raw_input):
                self.__queries.append(Query(raw_query))
            
            # Parse and execute each query 
            for query in self.__queries:
                if query.parse():
                    self.__types[query.get_type()].execute(self.__db, query.get_parsed_query())

            # Do the cleanup after the query (input) has been processed 
            self.__clean__()

    def __clean__(self):
        self.__raw_input = ""
        self.__queries = []

if __name__ == '__main__':
    print("Welcome to the MiniSQL monitor.")
    mini_sql_engine = Engine()
    mini_sql_engine.execute(sys.argv[1: ])