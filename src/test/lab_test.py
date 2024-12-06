import os.path
import sqlite3
import csv
import unittest

from src.main.main import load_and_clean_users, return_cursor, load_and_clean_call_logs, write_ordered_calls, write_user_analytics


class ProjectTests(unittest.TestCase):

    def setUp(self):

        self.cursor = return_cursor()

        # Delete any existing tables
        self.cursor.execute('DROP TABLE IF EXISTS users')
        self.cursor.execute('DROP TABLE IF EXISTS callLogs')

        # Create tables
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                                userId INTEGER PRIMARY KEY,
                                firstName TEXT,
                                lastName TEXT
                              )'''
                            )

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
                                callId INTEGER PRIMARY KEY, 
                                phoneNumber TEXT,
                                startTime INTEGER,
                                endTime INTEGER,
                                direction TEXT,
                                userId INTEGER,
                                FOREIGN KEY (userId) REFERENCES users(userId)
                            )'''
                            )

        print("Tables created from setUp.")

    # This test will a csv file of user data to assure incomplete records are left out.
    def test_users_table_has_clean_data(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the absolute path to testUsers.csv
        abs_file_path = os.path.join(base_dir,'testUsers.csv')
        # invoke load_and_clean_users, with testUsers.csv
        load_and_clean_users(abs_file_path)

        # select all records from the users table
        self.cursor.execute('''SELECT * FROM users''')

        # get the results and number of records
        results = self.cursor.fetchall()
        num_records = len(results)

        # assert that there are 2 records (the amount that should be left over)
        self.assertEqual(2, num_records)

        # assert that each result has the correct number of columns (3)
        # assert that the data coming back has a value for every column
        for result in results:
            self.assertEqual(3, len(result))
            for column in result:
                self.assertIsNotNone(column)

        # close the cursor
        self.cursor.close()

    # This test will a csv file of call data to assure incomplete records are left out.
    def test_calllogs_table_has_clean_data(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the absolute path to testUsers.csv
        abs_file_path = os.path.join(base_dir,'testCallLogs.csv')

        # invoke load_and_clean_call_logs, with testCallLogs.csv
        load_and_clean_call_logs(abs_file_path)

        # select all records from the callLogs table
        self.cursor.execute('''SELECT * FROM callLogs''')

        # get the results and number of records
        results = self.cursor.fetchall()

        num_records = len(results)

        # assert that there are 10 records (the amount that should be left over)
        self.assertEqual(10, num_records)
        # assert that the data coming back has a value for every column
        for result in results:
            self.assertEqual(6, len(result))
            for column in result:
                self.assertIsNotNone(column)

    # This test will test analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
    def test_user_analytics_are_correct(self):
        dir=os.path.dirname(os.path.abspath(__file__))

        # get absolute path of testCallLogs.csv
        test_call_logs_csv_path = os.path.join(dir, 'testCallLogs.csv')

        # get absolute path of testUserAnalytics.csv
        user_analytics_file_path=os.path.join(dir,'testUserAnalytics.csv')

        # List that will hold the contents of testUserAnalytics.csv
        user_analytics = []

        load_and_clean_call_logs(test_call_logs_csv_path)
        write_user_analytics(user_analytics_file_path)

        # Get the data from testUserAnalytics.csv
        with open(user_analytics_file_path, 'r') as file:

            # Skip the first line
            next(file)

            # Read the contents of the file line by line, saving them to user_analytics
            for line in file:
                user_analytics.append(line.strip().split(','))

        # order user_analytics by userId ascending
        user_analytics.sort(key=lambda x: int(x[0]))

        # ensure that the record with userId 4 has an avgDuration of 55 and a count of 2
        self.assertEqual(105.0, float(user_analytics[0][1]))
        self.assertEqual(4, int(user_analytics[0][2]))

        # ensure that the record with userId 2 has an avgDuration of 42.5 and a count of 4
        self.assertEqual(42.5, float(user_analytics[1][1]))
        self.assertEqual(4, int(user_analytics[1][2]))

        # ensure that the record with userId 1 has an avgDuration of 105.0 and a count of 4
        self.assertEqual(55.0, float(user_analytics[2][1]))
        self.assertEqual(2, int(user_analytics[2][2]))

    def test_call_logs_are_ordered(self):

        # Get the directory of the current test file
        test_dir = os.path.dirname(os.path.abspath(__file__))

        # get absolute path to testCallLogs.csv
        test_call_logs_path = os.path.join(test_dir, 'testCallLogs.csv')

        # Get absolute path to testOrderedCalls.csv
        test_ordered_calls_path = os.path.join(test_dir, 'testOrderedCalls.csv')

        # Load and clean call logs from testCallLogs.csv
        load_and_clean_call_logs(test_call_logs_path)

        #load_and_clean_call_logs("testCallLogs.csv")
        write_ordered_calls(test_ordered_calls_path)
        # List that will hold the contents of orderedCalls.csv
        ordered_calls = []
        #Get the data from orderedCalls.csv
        with open(test_ordered_calls_path, 'r') as file:
            # Skip the first line
            next(file)
            # Read the contents of the file line by line, saving them to ordered_calls
            for line in file:
                ordered_calls.append(line.strip().split(','))

        # Assert that the userId in the first record in ordered_calls is 1
        self.assertEqual(1, int(ordered_calls[0][5]))
        # Assert that the userId in the fifth record is 2
        self.assertEqual(2, int(ordered_calls[4][5]))
        # Assert that the userId in the last record is 4
        self.assertEqual(4, int(ordered_calls[-1][5]))

        # Assert that startTime in the first record is < the startTime in the second record
        self.assertTrue(int(ordered_calls[0][2]) < int(ordered_calls[1][2]))
        # Assert that the startTime in the penultimate record is < the startTime in the last record
        self.assertTrue(int(ordered_calls[-2][2]) < int(ordered_calls[-1][2]))