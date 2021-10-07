'''
* @Author: Samarth BM.
* @Date: 2021-10-07 12:38
* @Last Modified by: Samarth BM
* @Last Modified time: 2021-10-07 12:38
* @Title: To export and import database.
'''
import mysql.connector
import os
from dotenv import load_dotenv
from LogHandler import logger

load_dotenv()

class ExportImport:
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

    def export(self):
        """
        Description: This function is to export the database.
        
        """
        try:
            os.system('mysqldump -u root -p University > university-backup.sql')
           
        except Exception as e:
            logger.error(e)

    def import_database(self):
        """
        Description: This function is to import the database.
        
        """

        try:
            self.db_cursor.execute("CREATE DATABASE UniversityDuplicate")
            os.system('mysql -u root -p UniversityDuplicate < university-backup.sql')
            self.db_cursor.execute("SHOW DATABASES")

            for db in self.db_cursor:
                print(db[0])
           
        except Exception as e:
            logger.error(e)


if __name__ == "__main__":
    exportImport = ExportImport()
    exportImport.export()
    exportImport.import_database()