import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from sql_queries import create_view_queries, drop_view_queries


def drop_tables(cur, conn):
    """
    Drops the tables if they already exist in the Redshift database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def drop_views(cur, conn):
    """
    Drops the views if they already exist in the Redshift database
    """
    for query in drop_view_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the staging, fact, and dimesion tables in the Redshift database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def create_views(cur, conn):
    """
    Creates the views in the Redshift database
    """
    for query in create_view_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    drop_views(cur, conn)
    create_views(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()