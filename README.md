# Project: Python SQL API with File I/O

## Background 

-bit about python sql
-bit about file i/o

This project will be a backend for a hypothetical app that logs calls and user information for a call center. ______(more info here)___.

-Bit about the actual tasks involved in the project

## Database Tables 

The following tables will be initialized in your project's built-in database upon startup, defined in _____(file name)__________.

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

## Something about needing to use sqlite3 and csv libraries

- The app will already be a Python project with SQLite tables created at runtime. The callLogs.csv and users.csv files will be included in the resources folder for loading into the DB tables.


# User Stories

(to be organized/fleshed out into actual subsections)

- LOAD csv data into DB tables, cleaning up any junk data (just delete it? or edit it).
- SAVE analytic data for users into csv files. userAnalytics.csv
- SAVE call logs into csv files, ordered by (name?... userId?), then start time, excluding any junk data. orderedCallLogs.csv



