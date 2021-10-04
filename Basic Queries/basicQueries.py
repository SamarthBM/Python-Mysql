'''
* @Author: Samarth BM.
* @Date: 2021-10-04 14:48  
* @Last Modified by: Samarth BM
* @Last Modified time: 2021-10-04 14:48
* @Title: To connect mysql database with python.
'''

import mysql.connector
import os
from dotenv import load_dotenv
from LogHandler import logger

load_dotenv()

class BaiscQueries:
    """
        Description: This class contains all the methods to perform the CRUD operation.

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

    def create_database(self):
        """
        Description: This function is to create a database.

        """        

        try:

            self.db_cursor.execute("CREATE DATABASE python_sample1")
            self.db_cursor.execute("CREATE DATABASE students_sheet")
            self.db_cursor.execute("SHOW DATABASES")

            for db in self.db_cursor:
                print(db[0])

        except Exception as e:
            logger.error(e)

    def drop_database(self):
        """
        Description: This function is to delete any particular database.
        
        """ 

        try:

            self.db_cursor.execute("DROP DATABASE python_sample1")
            self.db_cursor.execute("SHOW DATABASES")

            for db in self.db_cursor:
                    print(db[0])
        
        except Exception as e:
            logger.error(e)
            

    def create_table(self):
        """
        Description: This function is to create a table.
        
        """ 

        try:

            self.db_cursor.execute("USE students_sheet")
            self.db_cursor.execute("CREATE TABLE StudentDetails(RollNo INT, Name VARCHAR(20), Marks INT, PRIMARY KEY (RollNo))")
            self.db_connection.commit()

        except Exception as e:
            logger.error(e)


    def describe_table(self):
        """
        Description: This function is to describe the table created.
        
        """ 
        try:
            self.db_cursor.execute("DESCRIBE StudentDetails")

            for tableInfo in self.db_cursor:
                print(tableInfo)

        except Exception as e:
            logger.error(e)


    def insert_into(self):
        """
        Description: This function is to insert the details into the table.
        
        """ 

        try:
            self.db_cursor.execute("USE students_sheet")
            self.db_cursor.execute("INSERT INTO StudentDetails(RollNo,Name,Marks) VALUES (18,'Samarth',88),(17,'Rahul',90)")
            self.db_connection.commit()

        except Exception as e:
            logger.error(e)

    def display_table(self):
        """
        Description: This function is to dispaly all values in table.
        
        """

        try: 

            self.db_cursor.execute("SELECT * FROM StudentDetails")

            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)


    def alter_table(self):
        """
        Description: This function is to alter the existing table.
        
        """

        try:

            self.db_cursor.execute("ALTER TABLE StudentDetails ADD Grade VARCHAR(1) AFTER Marks")    
            self.db_connection.commit()


        except Exception as e:
            logger.error(e)

    def update_details(self):
        """
        Description: This function is to update existing table taking name as the reference.
        
        """

        try:

            self.db_cursor.execute("UPDATE StudentDetails SET Marks = 82 WHERE Name = 'Rahul'")
            self.db_cursor.execute("UPDATE StudentDetails SET Grade = 'A' WHERE RollNo = 18")
            self.db_cursor.execute("UPDATE StudentDetails SET Grade = 'B' WHERE RollNo = 17")
            self.db_connection.commit()
            # self.display_table()
            
        except Exception as e:
            logger.error(e)

    def insert_many(self):
        """
        Description: This function is to insert many values to existing table.
        
        """

        try:

            sql = "INSERT INTO StudentDetails(RollNo,Name,Marks,Grade) VALUES(%s,%s,%s,%s)"
            val = [(5,'Karthik',35,'C'),(9,'Vivek',58,'B'),(12,'Jaya',22,'D')]
            self.db_cursor.executemany(sql,val)
            self.db_connection.commit()
            # self.display_table()

        except Exception as e:
            logger.error(e)

    def delete(self):
        """
        Description: This function is to delete a row from table using name.
        
        """
        try:

            self.db_cursor.execute("DELETE FROM StudentDetails WHERE Name = 'Vivek'")
            self.db_connection.commit()
            self.display_table()

        except Exception as e:
            logger.error(e)

    def orderby(self):
        """
        Description: This function is to order the table with respect to grade.
        
        """

        try:

            print("After oreder")
            self.db_cursor.execute("SELECT * FROM StudentDetails ORDER BY Grade")
            
            for details in self.db_cursor:
                print(details)

        except Exception as e:
            logger.error(e)

    def aggregate_functions(self):
        """
        Description: This function is to perform the aggregate funtions.
        Aggregate funtions are: COUNT, SUM, MAX, MIN, AVG
        
        """

        try:

            self.db_cursor.execute("SELECT COUNT(RollNo) FROM StudentDetails")
            result = self.db_cursor.fetchall()
            print("Total count: ",result[0][0])

            self.db_cursor.execute("SELECT SUM(Marks) FROM StudentDetails")
            result = self.db_cursor.fetchall()
            print("Sum of marks: ",result[0][0])

            self.db_cursor.execute("SELECT MAX(Marks) FROM StudentDetails")
            result = self.db_cursor.fetchall()
            print("Maximum marks: ",result[0][0])

            self.db_cursor.execute("SELECT MIN(Marks) FROM StudentDetails")
            result = self.db_cursor.fetchall()
            print("Minimum marks: ",result[0][0])

            self.db_cursor.execute("SELECT AVG(Marks) FROM StudentDetails")
            result = self.db_cursor.fetchall()
            print("Average marks: ",result[0][0])

        except Exception as e:
                logger.error(e) 
        

if __name__ == "__main__":
    queries = BaiscQueries()
    queries.create_database()
    queries.drop_database()
    queries.create_table()
    queries.describe_table()
    queries.insert_into()
    queries.update_details()
    queries.alter_table()
    queries.insert_many()
    queries.delete()
    queries.orderby()
    queries.aggregate_functions()