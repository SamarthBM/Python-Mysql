'''
* @Author: Samarth BM.
* @Date: 2021-10-06 00:31
* @Last Modified by: Samarth BM
* @Last Modified time: 2021-10-06 00:31
* @Title: To perform index in mysql.
'''

import mysql.connector
import os
from dotenv import load_dotenv
from LogHandler import logger

load_dotenv()

class Index:
    """
        Description: This class contains all the methods to perform index clause.

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

    def create_index(self):
        """
        Description: This function is to create a index for department column.
        
        """

        try:

            self.db_cursor.execute("USE University")
            self.db_cursor.execute("""CREATE INDEX college_department_idx 
                                    ON college(Department)""")

        except Exception as e:
            logger.error(e)

    def show_index(self):
        """
        Description: This function is to describe the clusterd and non-clustered index.
        
        """

        try:

            self.db_cursor.execute("SHOW INDEX FROM college")
            for i in self.db_cursor:
                print(i)

        except Exception as e:
            logger.error(e)

    def find_department(self):
        """
        Description: This function is to find the row, based on department.
        
        """
        try:

            self.db_cursor.execute("SELECT * FROM college WHERE Department = 'ME'")
            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)

    def explain(self):
        """
        Description: This function is to check the details of searching.
        
        """
        try:

            print("With indexing")
            self.db_cursor.execute("EXPLAIN SELECT * FROM college WHERE Department = 'ME'")
            for details in self.db_cursor:
                print(details)

            print("\nWithout indexing")
            self.db_cursor.execute("EXPLAIN SELECT * FROM college WHERE Name = 'Samarth'")
            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)

    def drop_index(self):
        """
        Description: This function is to delete the created index.
        
        """
        try:

            self.db_cursor.execute("ALTER TABLE college DROP INDEX college_department_idx")

        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    index = Index()
    index.create_index()
    index.show_index()
    index.find_department()
    index.explain()
    # index.drop_index()
