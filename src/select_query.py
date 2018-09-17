#!/usr/bin/python3

import sys
import collections
import sqlparse
import prettytable

from table import Table

class Select:
    """
        Class for implementing all the functions of SELECT query 
    """
    def __init__(self):
        self.__cols = []
        self.__distinct_cols = []
        self.__agg_functions = collections.OrderedDict()
        self.__join = False
        self.__join_cols = []
        self.__tables = []
        self.__conds = []
        self.__cond_conjunctions = []
        self.__query_table = Table("Query Table")
        self.__response_table = Table("Response Table")
        self.__error_messages = {
            1064: "ERROR 1064 (42000): You have an error in your SQL syntax; check the manual for the right syntax to use.",
            1146: "ERROR 1146 (42S02): Table doesn't exist",
            404: "ERROR 404 (1234): Data for table doesn't exist",
            500: "ERROR 500 (56789): Table couldn't be loaded"
        }

    def parse(self, parsed_query):
        data = [[], [], []]
        incomplete_query = 1
        idx = 0
        for x in parsed_query.tokens:
            if (isinstance(x, sqlparse.sql.Identifier) or isinstance(x, sqlparse.sql.Function)):
                data[idx].append(x)
            elif isinstance(x, sqlparse.sql.IdentifierList):
                for cf in x.get_identifiers():
                    data[idx].append(cf)
            elif str(x).upper() == 'FROM':
                idx += 1
            elif isinstance(x, sqlparse.sql.Where):
                idx += 1
                for w in x:
                    if isinstance(w, sqlparse.sql.Comparison):
                        data[idx].append(w)
                    elif (isinstance(w, sqlparse.sql.Token) and (str(w).upper() == 'AND' or str(w).upper() == 'OR')):
                        self.__cond_conjunctions.append(str(w).upper())
                    elif isinstance(w, sqlparse.sql.Token) and str(w) == ';':
                        incomplete_query = 0
            elif isinstance(x, sqlparse.sql.Token) and str(x) == ';':
                incomplete_query = 0
        if incomplete_query:
            print(self.__error_messages[1064])
            return -1

        for identifier in data[0]:
            if isinstance(identifier, sqlparse.sql.Identifier):
                self.__cols.append(str(identifier))
            elif isinstance(identifier, sqlparse.sql.Function):
                if str(identifier[0]).upper() == 'DISTINCT':
                    col = str(identifier[1])
                    col = col.lstrip('(')
                    col = col.rstrip(')')
                    self.__distinct_cols.append(col)
                else :
                    self.__agg_functions[identifier] = 0
        self.__tables = data[1]
        self.__conds = data[2]
        return 0

    def cartesian_product(self, table):
        self.__query_table.clean_cols()

        field_idx = len(self.__query_table.get_fields()) + 1
        for field in table.get_fields():
            self.__query_table.add_field(table.get_name() + "." + field, 'INTEGER', field_idx)
            field_idx += 1

        if len(self.__query_table.get_rows()) == 0:
            self.__query_table.set_rows(table.get_rows())
        else :
            rows = []
            for row1 in self.__query_table.get_rows():
                for row2 in table.get_rows():
                    row = row1 + row2
                    rows.append(row)
            self.__query_table.set_rows(rows)

    def union(self, rows):
        rows_union = set(self.__response_table.get_rows()).union(set(rows))
        self.__response_table.set_rows(list(rows_union))

    def intersection(self, rows):
        rows_intersection = set(self.__response_table.get_rows()).intersection(set(rows))
        self.__response_table.set_rows(list(rows_intersection))
    
    def compare(self, lhs, rhs, op):
        if op == '>':
            return lhs > rhs
        elif op == '<':
            return lhs < rhs
        elif op == '>=':
            return lhs >= rhs
        elif op == '<=':
            return lhs <= rhs
        elif op == '=':
            return lhs == rhs
        elif op == '<>':
            return lhs != rhs

    def process_conditions(self, db):
        self.__response_table.set_fields(self.__query_table.get_fields())
        
        response_rows_list = []
        for cond in self.__conds:
            data = []
            for x in cond:
                if isinstance(x, sqlparse.sql.Identifier) or (not x.is_whitespace and isinstance(x, sqlparse.sql.Token)):
                    data.append(str(x))
            lhs, op, rhs = [x for x in data]
            if lhs.split()[0] != rhs.split()[0] and op == '=':
                self.__join = True
                self.__join_cols.append((lhs, rhs))
            response_rows = []
            for row in self.__query_table.get_rows():
                try :
                    lhs_val = int(lhs)
                except ValueError:
                    lhs_val = row[self.__query_table.get_idx(lhs) - 1]
                try :
                    rhs_val = int(rhs)
                except ValueError:
                    rhs_val = row[self.__query_table.get_idx(rhs) - 1]
                if self.compare(lhs_val, rhs_val, op):
                    response_rows.append(row)
            response_rows_list.append(response_rows)
        if len(self.__cond_conjunctions) == 0:
            self.__response_table.set_rows(response_rows_list[0])
        else :
            self.union(response_rows_list[0])
            idx = 1
            for conjunction in self.__cond_conjunctions:
                if conjunction == 'OR':
                    self.union(response_rows_list[idx])
                elif conjunction == 'AND':
                    self.intersection(response_rows_list[idx])
                idx += 1

    def process_agg_functions(self, func, field):
        result = 0
        if func == 'MAX':
            result = -sys.maxsize - 1
            for col_val in self.__response_table.get_col(field):
                if col_val > result:
                    result = col_val
        elif func == 'MIN':
            result = sys.maxsize
            for col_val in self.__response_table.get_col(field):
                if col_val < result:
                    result = col_val
        elif func == 'SUM':
            result = 0
            for col_val in self.__response_table.get_col(field):
                result += col_val
        elif func == 'AVG':
            result = 0
            for col_val in self.__response_table.get_col(field):
                result += col_val
            result = result / len(self.__response_table.get_col(field))
        return result

    def print_response(self):
        response_table = prettytable.PrettyTable()
        if len(self.__cols) > 0 or len(self.__distinct_cols) > 0:
            row_col_idxs = []
            distinct_col_idxs = []
            distinct_col_vals = []
            response_fields = []
            for field in self.__distinct_cols:
                if field in self.__response_table.get_fields():
                    distinct_col_idxs.append(self.__response_table.get_field_val(field)['idx'])
                    row_col_idxs.append(self.__response_table.get_field_val(field)['idx'])
                    if len(self.__tables) == 1:
                        response_fields.append(str(self.__tables[0]) + "." + field)
                    else :
                        response_fields.append(field)
                else :
                    print("ERROR 1054 (42S22): Unknown column " + field + " in 'field list'")
                    return
            for field in self.__cols:
                if field in self.__response_table.get_fields():
                    row_col_idxs.append(self.__response_table.get_field_val(field)['idx'])
                    if len(self.__tables) == 1:
                        response_fields.append(str(self.__tables[0]) + "." + field)
                    else :
                        response_fields.append(field)
                else :
                    print("ERROR 1054 (42S22): Unknown column " + field + " in 'field list'")
                    return
            response_table.field_names = response_fields
            for row in self.__response_table.get_rows():
                response_row = []
                distinct_col_tuple = []
                for i in distinct_col_idxs:
                    distinct_col_tuple.append(row[i-1])
                
                if len(self.__distinct_cols) > 0:
                    if tuple(distinct_col_tuple) in distinct_col_vals:
                        continue
                    else :
                        distinct_col_vals.append(tuple(distinct_col_tuple))
                
                for i in row_col_idxs:
                    response_row.append(str(row[i-1]))
                response_table.add_row(response_row)
        elif len(self.__agg_functions) > 0 :
            response_fields = []
            response_row = []
            for agg_func in self.__agg_functions:
                response_fields.append(str(agg_func))
                field = str(agg_func[1])
                field = field.lstrip('(')
                field = field.rstrip(')')
                self.__agg_functions[agg_func] = self.process_agg_functions(str(agg_func[0]).upper(), field)
            for agg_func in self.__agg_functions:
                response_row.append(self.__agg_functions[agg_func])
            response_table.field_names = response_fields
            response_table.add_row(response_row)
        #else :
        #    response_fields = []
        #    for field in self.__response_table.get_fields():
        #        if len(self.__tables) == 1:
        #            response_fields.append(str(self.__tables[0]) + "." + field)
        #        else :
        #            response_fields.append(field)
        #    response_table.field_names = response_fields
        #    for row in self.__response_table.get_rows():
        #        response_row = []
        #        for i in range(len(row)):
        #            response_row.append(row[i])
        #        response_table.add_row(response_row)
        print(response_table)

    def __execute__(self, db):
        if len(self.__tables) == 0:
            print(self.__error_messages[1064])
            return 
        elif len(self.__tables) == 1:
            if not db.has_table(str(self.__tables[0])):
                print(self.__error_messages[1146])
                return
            else :
                self.__query_table = db.get_table(str(self.__tables[0]))
        else :
            for table in self.__tables:
                if not db.has_table(str(table)):
                    print(self.__error_messages[1146])
                    return
                else :
                    self.cartesian_product(db.get_table(str(table)))

        #print(self.__query_table.get_rows())
        # Process given conditions (if any) 
        if len(self.__conds) > 0:
            self.process_conditions(db)
        else :
            self.__response_table.set_fields(self.__query_table.get_fields())
            self.__response_table.set_rows(self.__query_table.get_rows())
        #print(self.__response_table.get_rows())

        if len(self.__distinct_cols) == 0 and len(self.__cols) == 0 and len(self.__agg_functions) == 0:
            for field in self.__query_table.get_fields():
                self.__cols.append(field)
        if (self.__join and self.__join_cols[0][1] in self.__cols):
            self.__cols.remove(self.__join_cols[0][1])
        # Print requested data 
        self.print_response()

        # Do the cleanup after the query has been processed 
        self.__clean__()

    def execute(self, db, query):
        if self.parse(query) != -1:
            self.__execute__(db)

    def __clean__(self):
        self.__cols = []
        self.__distinct_cols = []
        self.__agg_functions = collections.OrderedDict()
        self.__tables = []
        self.__conds = []
        self.__cond_conjunctions = []
        self.__query_table = Table("Query Table")
        self.__response_table = Table("Response Table")
