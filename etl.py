#import libraries
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads staging table from S3 storage to Redshift
    
    Input: Database cursor and connection instance
    Output: No specific Output
    """
    
    #Listing table names and initiate index
    staging_tables = ['staging_events', 'staging_songs']
    index = 0
    
    #Iterate through queries to load staging tables
    for query in copy_table_queries:        
        print('Copying staging table {}'.format(staging_tables[index]))
        index += 1
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function inserts columns to Redshift table
    
    Input: Database cursor and connection instance
    Output: No specific Output
    """
    
    #Listing table names and initiate index
    inserting_tables = ['songplays', 'users', 'songs', 'artists', 'time']
    index = 0
    
    #Iterate through queries to insert columns to Redshift tables
    for query in insert_table_queries:
        print('Inserting {} table'.format(inserting_tables[index]))
        index += 1
        cur.execute(query)
        conn.commit()


def main():
    """
    This is the main function of this script
    """
    
    #Read Config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(\
                                                                            config.get('CLUSTER','HOST'),\
                                                                            config.get('CLUSTER','DB_NAME'),\
                                                                            config.get('CLUSTER','DB_USER'),\
                                                                            config.get('CLUSTER','DB_PASSWORD'),\
                                                                            config.get('CLUSTER','DB_PORT')))
    cur = conn.cursor()
    
    #Load staging tables
    print('Loading staging tables')
    load_staging_tables(cur, conn)
    
    #Insert columns to Redshift tables
    print('Inserting staging tables')
    insert_tables(cur, conn)

    #Close the connection
    conn.close()


if __name__ == "__main__":
    main()