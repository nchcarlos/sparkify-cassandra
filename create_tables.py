import cassandra
from cassandra.cluster import Cluster

from queries import create_keyspace_stmt, create_tables_stmts, queries

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

def create_keyspace(session, keyspace):
    """
    Create a keyspace in the cluster.

    arguments:
    session - the Session object used to execute the KEYSPACE creation statement
    keyspace - the name of the KEYSPACE to create
    """
    try:
        session.execute(create_keyspace_stmt.format(keyspace))
    except Exception as e:
        print(e)

def drop_tables(session):
    """
    Drops all of the database tables.
    """
    for tab in ['session_library', 'song_library', 'user_library']:
        try:
            session.execute(f'DROP TABLE IF EXISTS {tab}')
        except Exception as e:
            print(e)

def create_tables(session):
    """
    Create all tables for the project.

    arguments:
    session - the Session object used to execute the KEYSPACE creation statement
    """
    for lib in create_tables_stmts:
        try:
            session.execute(lib)
        except Exception as e:
            print(e)

def main():
    project_keyspace = 'sparkify'
    cluster, session = connect_and_get_session()
    try:
        create_keyspace(session, project_keyspace)
        session.set_keyspace(project_keyspace)
        # drop the tables for a clean start
        drop_tables(session)
        create_tables(session)
    except Exception as e:
        print(e)
    finally:
        if cluster:
            cluster.shutdown()
        if session:
            session.shutdown()

if __name__ == '__main__':
    main()