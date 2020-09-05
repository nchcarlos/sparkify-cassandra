# Project: Data Modeling with Cassandra
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Datasets
The data for this project is provided in CSV files partitioned by date. The separate files are parsed and combined into as single file, ```event_datafile_new.csv```.

## Database Schema
The data will be modeled based on the following 3 questions.
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

### Tables
1. **session_library**
    - *session_id, item_in_session, artist_name, song_title, song_length*
    - ```PRIMARY KEY``` *session_id, item_in_session*
2. **user_library**
    - *user_id, session_id, item_in_session, artist_name, song_title, user_first_name, user_last_name text*
    - ```PRIMARY KEY``` *user_id, session_id, item_in_session*
3. **song_library**
    - song_title, user_last_name, user_first_name
    - ```PRIMARY KEY``` *song_title, user_last_name, user_first_name*

## ETL Pipeline
The ```etl.py``` script implements an ETL pipeline to extract data from ```event_datafile_new.csv``` and inserts the data into the appropriate tables.
The ```etl.py``` script will also execute queries that answer the 3 questions that were given in the project requirements.

### Queries
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

```sql
SELECT artist_name, song_title, song_length
FROM session_library
WHERE session_id = 338 and item_in_session = 4
```

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

```sql
SELECT artist_name, song_title, user_first_name, user_last_name
FROM user_library
WHERE user_id = 10 and session_id = 182
```

3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

```sql
SELECT user_first_name, user_last_name
FROM song_library
WHERE song_title = 'All Hands Against His Own'
```

To run the etl.py script, open a terminal window and run the following command:

```bash
python etl.py
```
