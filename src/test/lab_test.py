import os
import sqlite3
import csv
import unittest
import sys

# Add the main project directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.main.main import (
    load_and_clean_users,
    return_cursor,
    load_and_clean_call_logs,
    write_ordered_calls,
    write_user_analytics
)


class ProjectTests(unittest.TestCase):

    def setUp(self):
        """Set up the database connection and recreate tables for each test."""
        self.conn = sqlite3.connect(':memory:')  # Use an in-memory database for testing
        self.cursor = self.conn.cursor()

        # Recreate `users` and `callLogs` tables
        self.cursor.execute('DROP TABLE IF EXISTS users')
        self.cursor.execute('DROP TABLE IF EXISTS callLogs')

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

    def tearDown(self):
        """Tear down the database connection after each test."""
        self.conn.close()

    def test_users_table_has_clean_data(self):
        """Test that the `users` table contains only clean data after loading."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        abs_file_path = os.path.join(base_dir, 'testUsers.csv')

        # Load users into the database
        load_and_clean_users(abs_file_path, self.cursor)

        # Fetch all records from the `users` table
        self.cursor.execute('SELECT * FROM users')
        results = self.cursor.fetchall()

        # Assert that there are exactly 2 valid records in the `users` table
        self.assertEqual(len(results), 2)

        # Assert each record contains non-null values for all columns
        for result in results:
            self.assertEqual(len(result), 3)
            for column in result:
                self.assertIsNotNone(column)

    def test_calllogs_table_has_clean_data(self):
        """Test that the `callLogs` table contains only clean data after loading."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        abs_file_path = os.path.join(base_dir, 'testCallLogs.csv')

        # Load call logs into the database
        load_and_clean_call_logs(abs_file_path, self.cursor)

        # Fetch all records from the `callLogs` table
        self.cursor.execute('SELECT * FROM callLogs')
        results = self.cursor.fetchall()

        # Assert that there are exactly 10 valid records in the `callLogs` table
        self.assertEqual(len(results), 10)

        # Assert each record contains non-null values for all columns
        for result in results:
            self.assertEqual(len(result), 6)
            for column in result:
                self.assertIsNotNone(column)

    def test_user_analytics_are_correct(self):
        """Test that the user analytics are calculated correctly."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        test_call_logs_csv_path = os.path.join(base_dir, 'testCallLogs.csv')
        user_analytics_file_path = os.path.join(base_dir, 'testUserAnalytics.csv')

        # Load call logs and write analytics
        load_and_clean_call_logs(test_call_logs_csv_path, self.cursor)
        write_user_analytics(user_analytics_file_path, self.cursor)

        # Read analytics data from the output file
        user_analytics = []
        with open(user_analytics_file_path, 'r') as file:
            next(file)  # Skip header
            for line in file:
                user_analytics.append(line.strip().split(','))

        # Sort user analytics by userId
        user_analytics.sort(key=lambda x: int(x[0]))

        # Assert correctness of analytics data
        self.assertEqual(float(user_analytics[0][1]), 105.0)
        self.assertEqual(int(user_analytics[0][2]), 4)

        self.assertEqual(float(user_analytics[1][1]), 42.5)
        self.assertEqual(int(user_analytics[1][2]), 4)

        self.assertEqual(float(user_analytics[2][1]), 55.0)
        self.assertEqual(int(user_analytics[2][2]), 2)

    def test_call_logs_are_ordered(self):
        """Test that call logs are ordered correctly by userId and startTime."""
        base_dir = os.path.dirname(os.path.abspath(__file__))
        test_call_logs_path = os.path.join(base_dir, 'testCallLogs.csv')
        test_ordered_calls_path = os.path.join(base_dir, 'testOrderedCalls.csv')

        # Load call logs and write ordered calls
        load_and_clean_call_logs(test_call_logs_path, self.cursor)
        write_ordered_calls(test_ordered_calls_path, self.cursor)

        # Read ordered calls data from the output file
        ordered_calls = []
        with open(test_ordered_calls_path, 'r') as file:
            next(file)  # Skip header
            for line in file:
                ordered_calls.append(line.strip().split(','))

        # Assert correctness of order in the output
        self.assertEqual(int(ordered_calls[0][5]), 1)
        self.assertEqual(int(ordered_calls[4][5]), 2)
        self.assertEqual(int(ordered_calls[-1][5]), 4)

        self.assertTrue(int(ordered_calls[0][2]) < int(ordered_calls[1][2]))
        self.assertTrue(int(ordered_calls[-2][2]) < int(ordered_calls[-1][2]))


if __name__ == '__main__':
    unittest.main()
