import cassandra
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, BatchType
import pandas as pd

from queries import queries

def connect_and_get_session(cluster_ip='127.0.0.1'):
    """
    Creates and returns a Cluster and a Session object  .

    arguments:
    clust_ip -- the contact point to try connecting for cluster discovery
    """
    try:
        cluster = Cluster([cluster_ip])
        session = cluster.connect()
        return cluster, session
    except Exception as e:
        print(e)

def get_dataframe(data_file='event_datafile_new.csv'):
    """
    Read a CSV data file into a Pandas dataframe.
    The function also renames the columns to match the database schema.

    arguments;
    data_file - the CSV file to read
    """
    column_names = [
        'artist_name',
        'user_first_name',
        'user_gender',
        'item_in_session',
        'user_last_name',
        'song_length',
        'level',
        'location',
        'session_id',
        'song_title',
        'user_id'
    ]

    return pd.read_csv('event_datafile_new.csv', header=0, names=column_names)

def get_table_col_idxs(df):
    """
    Map the specific table columns to the matching index of the column in the
    dataframe. These indexes are used to insert the data to the database.

    arguments:
    df - the dataframe to map to
    """
    session_cols = [
        'session_id',
        'item_in_session',
        'artist_name',
        'song_title',
        'song_length'
    ]
    session_col_idxs = {c: i for i, c in enumerate(session_cols)}

    user_cols =  [
        'user_id',
        'item_in_session',
        'session_id',
        'artist_name',
        'song_title',
        'user_first_name',
        'user_last_name'
    ]
    user_col_idxs = {c: i for i, c in enumerate(user_cols)}

    song_cols = [
        'song_title',
        'user_last_name',
        'user_first_name'
    ]
    song_col_idxs = {c: i for i, c in enumerate(song_cols)}

    for i, col in enumerate(df.columns.values):
        if col in session_cols:
            session_col_idxs[col] = i
        if col in user_cols:
            user_col_idxs[col] = i
        if col in song_cols:
            song_col_idxs[col] = i

    return (
        list(session_col_idxs.values()),
        list(song_col_idxs.values()),
        list(user_col_idxs.values())
    )

def insert_data(session):
    """
    Insert data into the database.

    arguments:
    session - the Session object used to execute the KEYSPACE creation statement
    """
    df = get_dataframe()
    session_cols, song_cols, user_cols = get_table_col_idxs(df)

    batch = BatchStatement(batch_type=BatchType.UNLOGGED)
    batch_execute_at = 500
    stmt_count = 0

    for _, row in df.iterrows():
        # let's do the inserts
        #
        # we get the table specific data from the row by mapping the
        # __getitem__ function of the row values to the table column
        # indices.
        batch.add(queries['session_library']['insert'],
                  tuple(map(row.values.__getitem__, session_cols)))
        batch.add(queries['song_library']['insert'],
                  tuple(map(row.values.__getitem__, song_cols)))
        batch.add(queries['user_library']['insert'],
                  tuple(map(row.values.__getitem__, user_cols)))
        stmt_count += 3

        if stmt_count > batch_execute_at:
            try:
                session.execute(batch)
                batch.clear()
            except Exception as e:
                print(e)
            stmt_count = 0

def show_select_results(session):
    for tab in queries:
        print(f'SELECT FROM {tab}')
        try:
            rows = session.execute(queries[tab]['select'])
            for row in rows:
                print(row)
        except Exception as e:
            print(e)

def main():
    cluster, session = connect_and_get_session()
    try:
        session.set_keyspace('sparkify')
        insert_data(session)
        show_select_results(session)
    except Exception as e:
        print(e)
    finally:
        if cluster:
            cluster.shutdown()
        if session:
            session.shutdown()

if __name__ == '__main__':
    main()
