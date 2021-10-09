'''
* @Author: Samarth BM.
* @Date: 2021-10-07 19:00
* @Last Modified by: Samarth BM
* @Last Modified time: 2021-10-08 11:32
* @Title: To perform window functions.
'''
import mysql.connector
import os
from dotenv import load_dotenv
from LogHandler import logger

load_dotenv()

class WindowFunction:
    """
        Description: This class contains methods to export and import database.

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

    def database(self):
        """
        Description: This function is to select a database.
        
        """

        try:

            self.db_cursor.execute("USE students_sheet")

        except Exception as e:
            logger.error(e)

    def partition_by(self):
        """
        Description: This function is to perform partition by window function.
        
        """

        try:

            self.db_cursor.execute(""" SELECT RollNo,Name,Pass_out,Marks,SUM(Marks)
                                    OVER(PARTITION BY Pass_out) AS Total_marks
                                    FROM StudentDetails""")

            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)

    def analytical_function(self):
        """
        Description: This function is to perform analytical window function.
        NTILE takes a integer and groups the rows according to entered value.
        LEAD function takes the value of next row.
        LAG function takes the vlue from previous rwo.
        
        """

        try:
            print("\nAfter grouping")
            self.db_cursor.execute(""" SELECT RollNo,Name,Pass_out,Marks,
                                    NTILE(4) OVER() AS GroupNo
                                    FROM StudentDetails""")

            for details in self.db_cursor:
                print(details)

            print("\nAfter applying lead function")
            self.db_cursor.execute(""" SELECT RollNo,Name,Pass_out,Marks,
                                    LEAD(Marks,1) OVER(ORDER BY Pass_out) AS PreviousStudMarks
                                    FROM StudentDetails""")

            for details in self.db_cursor:
                print(details)

            print("\nAfter applying lag function")
            self.db_cursor.execute(""" SELECT RollNo,Name,Pass_out,Marks,
                                    LAG(Marks,1,0) OVER(ORDER BY Pass_out) AS NextStudMarks
                                    FROM StudentDetails""")

            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)
            
    def ranking(self):
        """
        Description: This function is to perform ranking window function.
        
        """

        try:

            print("\nAfter Ranking")
            self.db_cursor.execute("""SELECT RollNo,Name,Pass_out,Marks, 
                                    RANK() OVER(ORDER BY Marks) AS RankNo
                                    FROM StudentDetails""")

            for details in self.db_cursor:
                print(details)

            print("\nAfter Dense ranking")
            self.db_cursor.execute("""SELECT RollNo,Name,Pass_out,Marks, 
                                    DENSE_RANK() OVER(ORDER BY Marks) AS RankNo
                                    FROM StudentDetails""")

            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)


if __name__ == "__main__":
    windowFunction = WindowFunction()
    windowFunction.database()
    windowFunction.partition_by()
    windowFunction.analytical_function()
    windowFunction.ranking()