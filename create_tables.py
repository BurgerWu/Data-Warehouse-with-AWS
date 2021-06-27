#import libraries
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops the table in Redshift
    
    Input: Database cursor and connection instance
    Output: No specific Output
    """
    #Listing table names and initiate index
    dropping_tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
    index = 0
    
    #Iterate through queries to drop tables
    for query in drop_table_queries:
        print("Dropping {} table".format(dropping_tables[index]))
        index += 1
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates the table in Redshift
    
    Input: Database cursor and connection instance
    Output: No specific Output
    """
    #Listing table names and initiate index
    creating_tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
    index = 0
    
    #Iterate through queries to create tables
    for query in create_table_queries:
        print("Creating {} table".format(creating_tables[index]))
        index += 1
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function of this script
    """
    #Read Config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Connect to database
    print('Connecting to database')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(\
                                                                            config.get('CLUSTER','HOST'),\
                                                                            config.get('CLUSTER','DB_NAME'),\
                                                                            config.get('CLUSTER','DB_USER'),\
                                                                            config.get('CLUSTER','DB_PASSWORD'),\
                                                                            config.get('CLUSTER','DB_PORT')))
    cur = conn.cursor()
    
    #Drop tables
    print("Dropping existing tables")
    drop_tables(cur, conn)
    
    #Create tables
    print("Creating tables with desired schema")
    create_tables(cur, conn)
    
    #Close the connection
    conn.close()


if __name__ == "__main__":
    main()