'''
* @Author: Samarth BM.
* @Date: 2021-10-06 17:57
* @Last Modified by: Samarth BM
* @Last Modified time: 2021-10-06 17:57
* @Title: To create procedure in mysql.
'''

import mysql.connector
import os
from dotenv import load_dotenv
from LogHandler import logger

load_dotenv()

class Procedure:
    """
        Description: This class contains all the methods to create procedure.

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

    def switch_database(self):
        """
        Description: This function is to switch to students_sheet database.
        
        """
        try:
            self.db_cursor.execute("USE students_sheet")
        except Exception as e:
            logger.error(e)

    def procedure_without_parameter(self):
        """
        Description: This function is to create a procedure for merit list,
        where marks should be greater than 70. No parameter is used here.
        
        """
        try:

            self.db_cursor.execute("""CREATE PROCEDURE get_merit_student()
                                    BEGIN
                                    SELECT * FROM StudentDetails WHERE Marks > 70;
                                    SELECT COUNT(RollNO) AS 'Total_Students' FROM StudentDetails;
                                    END""")

            self.db_cursor.execute("CALL get_merit_student()")

            for result in self.db_cursor:
                print(result)
            
            #self.db_cursor.execute("DROP PROCEDURE get_merit_student")

        except Exception as e:
            logger.error(e)

    def procedure_with_in(self):
        """
        Description: This function is to create a procedure to get limited rows,
        here limit will be taken as input parameter.
        
        """

        try:

            self.db_cursor.execute("""CREATE PROCEDURE get_students(IN lim INT)
                                    BEGIN
                                    SELECT * FROM StudentDetails LIMIT lim;
                                    SELECT COUNT(RollNO) AS 'Total_Students' FROM StudentDetails;
                                    END""")

            self.db_cursor.execute("CALL get_students(3)")

            for result in self.db_cursor:
                print(result)

            #self.db_cursor.execute("DROP PROCEDURE get_students")

        except Exception as e:
            logger.error(e)

    def procedure_with_out(self):
        """
        Description: This function is to create a procedure to get highest marks,
        here output will be stored to parameter.
        
        """

        try:

            self.db_cursor.execute("""CREATE PROCEDURE get_max_marks(OUT topper INT)
                                    BEGIN
                                    SELECT MAX(Marks) INTO topper FROM StudentDetails;                         
                                    END""")

            self.db_cursor.execute("CALL get_max_marks(@M)")
            self.db_cursor.execute("SELECT @M")
    
            for result in self.db_cursor:
                print("Higest marks:",result[0])

            #self.db_cursor.execute("DROP PROCEDURE get_max_marks")

        except Exception as e:
            logger.error(e)

    def procedure_with_in_out(self):
        """
        Description: This function is to create a procedure to get highest marks,
        here output will be stored to parameter.
        
        """
       
        try:

            self.db_cursor.execute("""CREATE PROCEDURE display_marks(INOUT var INT)
                                    BEGIN
                                    SELECT Marks INTO var FROM StudentDetails WHERE RollNo = var;                         
                                    END""")

            self.db_cursor.execute("SET @M = '18'")
            self.db_cursor.execute("CALL display_marks(@M)")     
            self.db_cursor.execute("SELECT @M")

            for result in self.db_cursor:
                print(result[0])
    
             #self.db_cursor.execute("DROP PROCEDURE display_marks")

        except Exception as e:
            logger.error(e)

        
if __name__ == "__main__":
    procedure = Procedure()
    procedure.switch_database()
    # procedure.procedure_without_parameter()
    #procedure.procedure_with_in()
    #procedure.procedure_with_out()
    procedure.procedure_with_in_out()