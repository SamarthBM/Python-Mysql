'''
* @Author: Samarth BM.
* @Date: 2021-10-05 17:32
* @Last Modified by: Samarth BM
* @Last Modified time: 2021-10-05 17:32
* @Title: To perform all joins in mysql.
'''

import mysql.connector
import os
from dotenv import load_dotenv
from LogHandler import logger

load_dotenv()

class Joins:
    """
        Description: This class contains all the methods to perform different joins in mysql.

    """

    def __init__(self):

        host=os.getenv("HOST")
        user=os.getenv('USER3')
        password=os.getenv('PASSWD')
        auth_plugin=os.getenv('PLUGIN') 
        
        self.db_connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            auth_plugin=auth_plugin    
            )
        
        self.db_cursor = self.db_connection.cursor()

    def display_tables(self):
        """
        Description: This function is to display the values of college and hostel table.
        
        """

        try:

            self.db_cursor.execute("USE University")
            self.db_cursor.execute("SELECT * FROM college")

            print("College table")
            for details in self.db_cursor:
                print(details)

            self.db_cursor.execute("SELECT * FROM hostel")

            print("\nHostel table")
            for details in self.db_cursor:
                print(details)

        except Exception as e:
                logger.error(e)


    def inner_join(self):
        """
        Description: This function is to perform inner join.
        The INNER JOIN keyword selects the records that have matching values in both tables.
        
        """
        try:

            print("\nInner join:")
            self.db_cursor.execute("""SELECT college.College_ID, college.Name, hostel.RoomNO
                                    FROM college
                                    INNER JOIN hostel
                                    ON college.College_ID = hostel.Hostel_ID""")

            print("\n{}   {}    {}".format("ID","Name","Room No"))
            for details in self.db_cursor:
                    print(details)

            self.db_cursor.execute("""SELECT college.College_ID, college.Name, hostel.RoomNO
                                    FROM college
                                    INNER JOIN hostel
                                    ON college.College_ID = hostel.Hostel_ID
                                    WHERE Department = 'CSE'""")

            print("\nSelecting only CSE department")
            print("{}   {}    {}".format("ID","Name","Room No"))
            for details in self.db_cursor:
                    print(details)


        except Exception as e:
            logger.error(e)

    def left_join(self):
        """
        Description: This function is to perform left join.
        The LEFT JOIN keyword returns all records from the left table, 
        and the matching records from the right table.
        
        """
        
        try:

            print("\nLeft join:")
            self.db_cursor.execute("""SELECT college.College_ID, college.Name, hostel.RoomNO
                                    FROM college
                                    LEFT JOIN hostel
                                    ON college.College_ID = hostel.Hostel_ID""")

            print("\n{}   {}    {}".format("ID","Name","Room No"))
            for details in self.db_cursor:
                    print(details)

        except Exception as e:
            logger.error(e)

    def right_join(self):
        """
        Description: This function is to perform Right join.
        The RIGHT JOIN keyword returns all records from the right table, 
        and the matching records from the left table.
        
        """

        try:

            print("\nRight join:")
            self.db_cursor.execute("""SELECT college.College_ID, college.Name, hostel.RoomNO
                                    FROM college
                                    RIGHT JOIN hostel
                                    ON college.College_ID = hostel.Hostel_ID""")

            print("\n{}   {}    {}".format("ID","Name","Room No"))
            for details in self.db_cursor:
                    print(details)

        except Exception as e:
            logger.error(e)

    def cross_join(self):
        """
        Description: This function is to perform Cross join.
        The CROSS JOIN keyword returns all records from both tables.
        
        """

        try:

            print("\nCross join:")
            self.db_cursor.execute("""SELECT college.College_ID, college.Name, hostel.RoomNO
                                    FROM college
                                    CROSS JOIN hostel""")

            print("\n{}   {}    {}".format("ID","Name","Room No"))
            for details in self.db_cursor:
                    print(details)

        except Exception as e:
            logger.error(e)

    def self_join(self):
        """
        Description: This function is to perform self join.
        The self join is used to create a new table from existing table.
        
        """
        try:

            print("\nSelf join:")
            self.db_cursor.execute("""SELECT a.Name as Name, b.Name as Friend 
                                    FROM college a 
                                    LEFT JOIN college b 
                                    ON a.Friend_ID = b.College_ID""")

            print("\n{}   {} ".format("Name","Friend"))
            for details in self.db_cursor:
                    print(details)

        except Exception as e:
            logger.error(e)

    def full_outer_join(self):
        """
        Description: This function is to perform full outer join.
        
        """
        try:

            print("\nFull outer join join:")
            self.db_cursor.execute("""SELECT * FROM college
                                    LEFT JOIN hostel ON college.College_ID = hostel.Hostel_ID
                                    UNION ALL 
                                    SELECT * FROM college
                                    RIGHT JOIN hostel ON college.College_ID = hostel.Hostel_ID""")

            for details in self.db_cursor:
                    print(details)

        except Exception as e:
            logger.error(e)


if __name__ == "__main__":
    join = Joins()
    join.display_tables()
    join.inner_join()
    join.left_join()
    join.right_join()
    join.cross_join()
    join.self_join()
    join.full_outer_join()