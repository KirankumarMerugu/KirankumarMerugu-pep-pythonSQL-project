# Project: Python SQL API with File I/O

## Background 

There are two halves in a full-stack application: the front end, responsible for user interaction and display, and the backend, which handles data storage and processing. In this project, we'll work on the backend, creating an application to manage data in a hypothetical call center. Our application will handle data for users and calls, and it will use that data to provide analytics and data ordering functionality.

The primary technologies you will leverage in this project are Python, SQL, and File I/O with .csv files. The project will be written in Python. Data will be loaded into an in-memory SQLite database from existing .csv files, and analytic data will be saved into new .csv files. 

## Database Tables 

The following tables will be initialized in your project's built-in database upon startup.

### users
```
userId INTEGER PRIMARY KEY,
firstName TEXT
lastName TEXT
```

### callLogs
```
callId INTEGER PRIMARY KEY,
phoneNumber TEXT,
startTimeEpoch INTEGER,
endTimeEpoch INTEGER,
callDirection TEXT,
userId INTEGER,
FOREIGN KEY (userId) REFERENCES users(userId)
```

Note - by specifying IDs as primary keys, the id value should auto-increment for each new record.

# Technical Requirements

### SQLite

- The app will already be a Python project with SQLite tables created at runtime. 
- You will be responsible for cleaning and inserting data into the database, as well as selecting and modifying that data for analysis.  

### CSV

- The callLogs.csv and users.csv files will be included in the resources folder for loading into the DB tables.
- You will be responsible for loading the data from these existing files into the database, as well as writing analytic data to new files.

# User Stories


### Load user data into users table
- Load the users.csv file found in /resources into the users table
- Clean the data before insertion. In this project, you just have to leave out any records with missing values or too many values.
- HINT: For every record in users.csv, make sure it has the correct number of fields and no empty values before inserting into the Database.

### Load call data into callLogs table
- Load the callLogs.csv file found in /resources into the callLogs table 
- Clean the data before insertion. In this project, you just have to leave out any records with missing values or too many values.
- HINT: For every record in callLogs.csv, make sure it has the correct number of fields and no empty values before inserting into the Database.

### Save user analytic data into userAnalytics.csv
- Save analytic data for users into a csv file. The file must be named userAnalytics.csv, and it must be in the /resources folder
- Records must include userId, avgDuration, numCalls. Example:
  ```
  userId,avgDuration,numCalls
  1,105.0,4
  ```
- HINT: This data will be selected from the callLogs table.
- HINT 2: Dictionaries will be very helpful for matching data with userIds. Consider one for {userId, average call duration} and one for {userId, number of calls}. 

### Save ordered call logs into orderedCallLogs.csv
- Save call logs into csv files, ordered by userId, then start time. The file must be named orderedCallLogs.csv
- HINT: This data will be selected from the callLogs table.
- HINT 2: You can make use of ORDER BY to greatly simplify your python logic

*General note - each of these functions take a "file_path" parameter. You will not need to edit this variable, but it will be used to accomplish each implementation. See main() for an example of the function invocations with file paths from /resources.

