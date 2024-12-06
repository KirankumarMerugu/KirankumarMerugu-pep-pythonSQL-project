import sqlite3
import csv
import unittest

from src.main import load_and_clean_users, return_cursor, load_and_clean_call_logs


class ProjectTests(unittest.TestCase):

    def setUp(self):

        self.cursor = return_cursor()

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

    # This test will use two different csv files of user data to assure incomplete records are left out.
    def test_users_table_has_clean_data(self):

        # invoke load_and_clean_users, with testUsers.csv
        load_and_clean_users("testUsers.csv")

        # select all records from the users table
        self.cursor.execute('''SELECT * FROM users''')

        # get the results and number of records
        results = self.cursor.fetchall()
        num_records = len(results)

        # assert that there are 2 records (the amount that should be left over)
        self.assertEqual(2, num_records)

        # assert that the data coming back has a value for every column
        for result in results:
            self.assertEqual(3, len(result))
            for column in result:
                self.assertIsNotNone(column)

        # close the cursor
        self.cursor.close()

    def test_calllogs_table_has_clean_data(self):

        # invoke load_and_clean_users, with testUsers.csv and testUsers2.csv
        load_and_clean_call_logs("testCallLogs.csv")

        # select all records from the users table
        self.cursor.execute('''SELECT * FROM callLogs''')

        # get the results and number of records
        results = self.cursor.fetchall()

        num_records = len(results)

        # assert that there are 2 records (the amount that should be left over)
        self.assertEqual(10, num_records)
        # assert that the data coming back has a value for every column
        for result in results:
            self.assertEqual(6, len(result))
            for column in result:
                self.assertIsNotNone(column)


    def test_user_analytics_are_correct(self):

        # List that will hold the contents of userAnalytics.csv
        user_analytics = []

        # Get the data from userAnalytics.csv
        with open( "../resources/userAnalytics.csv", 'r') as file:

            # Skip the first line
            next(file)

            # Read the contents of the file line by line, saving them to user_analytics
            for line in file:
                user_analytics.append(line.strip().split(','))

        # ensure that the record with userId 1 has an avgDuration of 105 and a count of 4
        self.assertEqual(105.0, float(user_analytics[0][1]))
        self.assertEqual(4, int(user_analytics[0][2]))

        # ensure that the record with userId 2 has an avgDuration of 40 and a count of 5
        self.assertEqual(40.0, float(user_analytics[1][1]))
        self.assertEqual(5, int(user_analytics[1][2]))

        # ensure that the record with userId 5 has an avgDuration of 70 and a count of 1
        self.assertEqual(70.0, float(user_analytics[4][1]))
        self.assertEqual(1, int(user_analytics[4][2]))

    def test_call_logs_are_ordered(self):

        # List that will hold the contents of orderedCalls.csv
        ordered_calls = []

        # Get the data from orderedCalls.csv
        with open("../resources/orderedCalls.csv", 'r') as file:

            # Skip the first line
            next(file)

            # Read the contents of the file line by line, saving them to ordered_calls
            for line in file:
                ordered_calls.append(line.strip().split(','))

        # Assert that the userId in the first record in ordered_calls is 1
        self.assertEqual(1, int(ordered_calls[0][5]))
        # Assert that the userId in the fifth record is 2
        self.assertEqual(2, int(ordered_calls[4][5]))
        # Assert that the userId in the last record is 5
        self.assertEqual(5, int(ordered_calls[-1][5]))

        # Assert that startTime in the first record is < the startTime in the second record
        self.assertTrue(int(ordered_calls[0][2]) < int(ordered_calls[1][2]))
        # Assert that the startTime in the penultimate record is < the startTime in the last record
        self.assertTrue(int(ordered_calls[-2][2]) < int(ordered_calls[-1][2]))
