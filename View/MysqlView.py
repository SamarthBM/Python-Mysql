'''
* @Author: Samarth BM.
* @Date: 2021-10-05 22:32
* @Last Modified by: Samarth BM
* @Last Modified time: 2021-10-05 22:32
* @Title: To perform view in mysql.
'''

import mysql.connector
import os
from dotenv import load_dotenv
from LogHandler import logger

load_dotenv()

class View:
    """
        Description: This class contains all the methods to perform view clause.

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

    def create_view(self):
        """
        Description: This function is to create a view for college table.
        
        """

        try:

            self.db_cursor.execute("USE University")
            self.db_cursor.execute("""CREATE VIEW StudentDetails AS
                                    SELECT Name,Department
                                    FROM college""")

        except Exception as e:
            logger.error(e)

    def display_view(self):
        """
        Description: This function is to display the created view.
        
        """

        try:

            self.db_cursor.execute("SELECT * FROM StudentDetails")

            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)

    def alter_view(self):
        """
        Description: This function is to update the created view.
        
        """
        try:

            self.db_cursor.execute("""ALTER VIEW StudentDetails AS
                                    SELECT Name,Department
                                    FROM college
                                    WHERE Department = 'ME'""")

        except Exception as e:
            logger.error(e)
        
    def drop_view(self):
        """
        Description: This function is to delete the created view.
        
        """
        try:

            self.db_cursor.execute("DROP VIEW StudentDetails")

        except Exception as e:
            logger.error(e)

if __name__ == "__main__":
    view = View()
    view.create_view()
    # view.display_view()
    view.alter_view()
    view.display_view()
    # view.drop_view()