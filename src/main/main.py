import csv
import sqlite3


def create_tables(cursor):
    """Create tables if they do not exist."""
    # Create `users` table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY AUTOINCREMENT,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # Create `callLogs` table
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
                        callId INTEGER PRIMARY KEY AUTOINCREMENT,
                        phoneNumber TEXT NOT NULL,
                        startTime INTEGER NOT NULL,
                        endTime INTEGER NOT NULL,
                        direction TEXT NOT NULL,
                        userId INTEGER NOT NULL,
                        FOREIGN KEY (userId) REFERENCES users(userId)
                      )'''
                   )


def load_and_clean_users(file_path, cursor):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header

        users = []
        for row in csv_reader:
            if len(row) == 2 and row[0].strip() and row[1].strip():
                users.append((row[0].strip(), row[1].strip()))

        cursor.executemany(
            'INSERT INTO users (firstName, lastName) VALUES (?, ?)',
            users
        )


def load_and_clean_call_logs(file_path, cursor):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header

        call_logs = []
        for log in csv_reader:
            if len(log) == 5 and all(item.strip() for item in log):
                call_logs.append((
                    log[0].strip(), int(log[1]), int(log[2]),
                    log[3].strip(), int(log[4])
                ))

        cursor.executemany(
            '''INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId)
               VALUES (?, ?, ?, ?, ?)''',
            call_logs
        )


def write_user_analytics(csv_file_path, cursor):
    cursor.execute('SELECT startTime, endTime, userId FROM callLogs')
    user_stats = {}

    for record in cursor:
        start_time, end_time, user_id = record
        call_duration = end_time - start_time

        if user_id not in user_stats:
            user_stats[user_id] = {'total_time': 0, 'call_count': 0}

        user_stats[user_id]['total_time'] += call_duration
        user_stats[user_id]['call_count'] += 1

    final_data = [
        (user_id, stats['total_time'] / stats['call_count'], stats['call_count'])
        for user_id, stats in user_stats.items()
    ]

    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])
        writer.writerows(final_data)


def write_ordered_calls(csv_file_path, cursor):
    cursor.execute('SELECT callId, phoneNumber, startTime, endTime, direction, userId FROM callLogs')
    all_calls = cursor.fetchall()

    sorted_calls = sorted(all_calls, key=lambda call: (call[5], call[2]))

    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])
        writer.writerows(sorted_calls)

def return_cursor():
    connection = sqlite3.connect(':memory:')  # Use an in-memory database for testing
    return connection.cursor()

def main():
    # Connect to the SQLite in-memory database
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Create tables
    create_tables(cursor)

    # Load data and generate outputs
    load_and_clean_users('../../resources/users.csv', cursor)
    load_and_clean_call_logs('../../resources/callLogs.csv', cursor)
    write_user_analytics('../../resources/userAnalytics.csv', cursor)
    write_ordered_calls('../../resources/orderedCalls.csv', cursor)

    # Uncomment to debug
    # select_from_users_and_call_logs(cursor)

    # Commit changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()
