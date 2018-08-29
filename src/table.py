#!/usr/bin/python3

import collections

class Table:
    """
        Table class which stores data elements (values) in the form of tuples (rows) with 
        each value belonging to a certain field in the table 
    """
    def __init__(self, name):
        self.__name = name
        self.__fields = collections.OrderedDict()
        self.__cols = {}
        self.__rows = []

    def get_name(self):
        return self.__name

    def get_fields(self):
        return self.__fields

    def set_fields(self, fields):
        self.__fields = collections.OrderedDict()
        for field in fields:
            self.__fields[field] = fields[field]
            self.__cols[field] = []

    def get_field_val(self, field):
        return self.__fields[field]

    def add_field(self, field, type, field_num):
        self.__fields[field] = {'type': type, 'idx': field_num}
        self.__cols[field] = []

    def get_idx(self, field):
        return self.__fields[field]['idx']

    def get_rows(self):
        return self.__rows

    def set_rows(self, rows):
        self.__rows = rows
        for row in self.__rows:
            idx = 0
            for field in self.__fields:
                self.__cols[field].append(row[idx])
                idx += 1

    def add_row(self, row):
        self.__rows.append(row)

    def get_col(self, field):
        return self.__cols[field]

    def add_col_value(self, field, value):
        self.__cols[field].append(value)

    def clean_cols(self):
        for field in self.__cols:
            self.__cols[field] = []

    def load(self):
        with open("../sample-files/" + self.__name + '.csv') as file:
            for line in file.readlines():
                row = tuple([int(x.strip().strip('"')) for x in line.strip().split(',')])
                self.__rows.append(row)
                idx = 0
                for field in self.__fields:
                    self.__cols[field].append(row[idx])
                    idx += 1
