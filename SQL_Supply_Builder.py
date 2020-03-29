import sqlite3
from _sqlite3 import Error
import pandas as pd
from pandas import DataFrame
from _sqlite3 import OperationalError

"""A Program that creates tables, inserts data from .csv files, adds single lines and able to delete
    single lines without having to use SQL code. This program also works with EXP_Dates, but only if the 
    table names are in Box0, Box1 etc. format. """


def create_connection(db_file):
    """Creates connection to database
        returns error if no connection is established"""

    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def print_data(conn, table):
    """Returns all data in a table"""

    c = conn.cursor()
    c.execute("""Select * FROM {}""".format(table))
    df = DataFrame(c.fetchall(), columns=['Category', 'Product', 'Packaging', 'Volume_Size', 'Quantity', 'EXP'])
    return print(df)


def show_tables(conn):
    """Shows all tables for connected database"""

    c = conn.cursor()
    c.execute("""SELECT name FROM sqlite_master
        WHERE type='table'
        ORDER BY name;""")
    df = DataFrame(c.fetchall(), columns=['name'])
    return print(df)


def make_table(conn, table_name):
    """Makes new tables in the database with pre-established columns"""

    c = conn.cursor()
    c.execute("""CREATE TABLE '{}' (
        "Category"	TEXT NOT NULL,
        "Product"	TEXT NOT NULL,
        "Packaging"	TEXT,
        "Volume_Size"	TEXT,
        "Quantity"	INTEGER,
        "EXP"	INTEGER DEFAULT 'NA'
        )""".format(table_name))
    conn.commit()
    print_data(conn, table_name)


def csv_insert(conn, file, table):
    """Fills a table from a .csv file"""

    c = conn.cursor()

    read_clients = pd.read_csv(r"{}".format(file))  # CSV FIle
    read_clients.to_sql('{}'.format(table), conn, if_exists='append', index=False)

    c.execute("""INSERT INTO {0} (CATEGORY, PRODUCT, PACKAGING, VOLUME_SIZE, QUANTITY, EXP)
            SELECT CATEGORY, PRODUCT, PACKAGING, VOLUME_SIZE, QUANTITY, EXP
            FROM {1}""".format(table, table))

    conn.commit()
    print_data(conn, table)


def add_item(conn, table):
    """Adds a single item to a single table"""

    c = conn.cursor()
    category = input('Category? ')
    product = input('Product? ')
    packaging = input('Packaging? ')
    volume_size = input('Volume or size? ')
    quantity = input('Quantity? ')
    exp = input('Expiration date in Year, Month, Day? ')
    c.execute("""Insert into '{0}' (CATEGORY, PRODUCT, PACKAGING, VOLUME_SIZE, QUANTITY, EXP)
        VALUES ('{1}', '{2}', '{3}', '{4}', '{5}', '{6}')""".format(table, category, product, packaging,
                                                                    volume_size, quantity, exp))

    conn.commit()
    print_data(conn, table)


def remove_item(conn, table, item):
    """Removes a single item from a table"""

    c = conn.cursor()
    c.execute("""Delete from {0} where product = {1}""".format(table, item))

    conn.commit()
    print_data(conn, table)


def main():
    session = 'open'
    while session == 'open':
        try:
            test = input("What would you like to do? \n"
                         "Insert file into table? \n"
                         "Make a new table? \n"
                         "Remove item? \n"
                         "Insert single item? \n")

            database = r".../Test_DB.db"
            conn = create_connection(database)

            if ("insert" and "file") in test.lower():
                complete = 'no'
                while complete == ("no" or 'No'):
                    file_location = input("Where is the file? \n")
                    show_tables(conn)
                    imp_table = input("Which table? \n")

                    csv_insert(conn, file_location, imp_table)
                    complete = input("Are you finished? \n")

            elif ("make" or "table") in test.lower():
                new_table = 'no'
                while new_table == ('no' or 'No'):
                    print("Current Tables:")
                    show_tables(conn)
                    table_name = input("What table name? \n")

                    make_table(conn, table_name)
                    new_table = input("Finished adding tables? \n")

            elif "remove" in test.lower():
                remove = 'no'
                while remove == ('no' or 'No'):
                    show_tables(conn)
                    table = input('What table is this in? \n')
                    print_data(conn, table)
                    item = input('What item are we removing? \n')

                    remove_item(conn, table, item)
                    remove = input('Finished removing items from {}? \n'.format(table))

            elif ("insert" or "single") in test.lower():
                item_add = 'no'
                while item_add == ('no' or 'No'):
                    show_tables(conn)
                    table = input('What table would you like to add to? \n')
                    # todo add check if var in table list
                    # if table not in show_tables(conn):
                    #     raise OperationalError

                    add_item(conn, table)
                    item_add = input('Finished adding? \n')

            else:
                raise OperationalError

            conn.commit()
            conn.close()
            session = 'closed'
        except OperationalError:
            print("lets try that again! \n")


if __name__ == '__main__':
    main()
