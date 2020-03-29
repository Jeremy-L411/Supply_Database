import sqlite3
from pandas import DataFrame
import datetime
from datetime import date

"""Script to show which products are expiring in a specified amount of time.
    The preset time is 1 week and 2 weeks. It iterates through the tables
    with the root name Box. Currently set up for 5 boxes. Further development
    will have a range of boxes. """


today = date.today()
week = datetime.timedelta(weeks=1)
one_week = today + week
weeks = datetime.timedelta(weeks=2)
two_weeks = today + weeks

conn = sqlite3.connect('.../Test_DB.db')
c = conn.cursor()

box = 0
while box < 6:
    print("Products Expiring in box{} by {} are: \n".format(box, one_week))
    c.execute("""SELECT * FROM BOX{} WHERE EXP BETWEEN '{}' and '{}'""".format(box, today, one_week))
    df = DataFrame(c.fetchall(), columns=['Category', 'Product', 'Packaging', 'Volume_Size', 'Quantity', 'EXP'])
    print(df)
    print(" ")

    print("Products Expiring in Box{} by {} are: \n".format(box, two_weeks))
    c.execute("""SELECT * FROM BOX{} WHERE EXP BETWEEN '{}' and '{}'""".format(box, today, two_weeks))
    df = DataFrame(c.fetchall(), columns=['Category', 'Product', 'Packaging', 'Volume_Size', 'Quantity', 'EXP'])
    print(df)
    print("_"*30)
    box += 1