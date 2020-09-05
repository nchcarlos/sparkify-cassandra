create_keyspace_stmt="""
CREATE KEYSPACE IF NOT EXISTS {0}
WITH REPLICATION = {{
    'class': 'SimpleStrategy',
    'replication_factor': 1
}}"""

session_library = """
CREATE TABLE IF NOT EXISTS session_library
(
    session_id int,
    item_in_session int,
    artist_name text,
    song_title text,
    song_length int,
    PRIMARY KEY (session_id, item_in_session)
)
"""

user_library = """
CREATE TABLE IF NOT EXISTS user_library
(
    user_id int,
    item_in_session int,
    session_id int,
    artist text,
    song_title text,
    user_first_name text,
    user_last_name text,
    PRIMARY KEY (user_id, item_in_session, session_id)
) WITH CLUSTERING ORDER BY (item_in_session ASC, session_id ASC)
"""

song_library = """
CREATE TABLE IF NOT EXISTS song_library
(
    song_title text,
    user_first_name text,
    user_last_name text,
    PRIMARY KEY (song_title)
)
"""

create_tables_stmts = [session_library, user_library, song_library]

# Create queries to ask the following three questions of the data
# 1. Give me the artist, song title and song's length in the music app
#       history that was heard during sessionId = 338, and
#       itemInSession = 4
#
# session library
session_lib_query = """
SELECT artist_name, song_title, song_lenght
FROM session_library
WHERE session_id = 338 and item_in_session = 4
"""

# 2. Give me only the following: name of artist, song (sorted by
#       itemInSession) and user (first and last name) for userid = 10,
#       sessionid = 182
#
# user library
user_lib_query = """
SELECT artist_name, song_title, user_first_name, user_last_name
FROM user_library
WHERE user_id = 10 and session_id = 182
"""

# 3. Give me every user name (first and last) in my music app history who
#       listened to the song 'All Hands Against His Own'
#
# song library
song_lib_query = """
SELECT user_first_name, user_last_name
FROM song_library
WHERE song_title = 'All Hands Against His Own'
"""

queries = {
    'session_library': {
        'insert': '',
        'select': session_lib_query
    },
    'user_library': {
        'insert': '',
        'select': user_lib_query
    },
    'song_library': {
        'insert': '',
        'select': song_lib_query
    }
}