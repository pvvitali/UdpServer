import psycopg2
from psycopg2 import Error

def write_to_database(str):

    try:

        conn = psycopg2.connect(
            host='195.133.145.188',
            port='5432',
            user='vitali',
            password='vit777pv',
            database='stdata'
        )

        cursor = conn.cursor()
        str_query = f"insert into alldata( strdata ) values ( '{str}' )" 
        cursor.execute(str_query)
        conn.commit()
        
    except(Exception, Error) as error:
        print("Error from work to PostgresSQL ", error)
    finally:
        if conn:
            cursor.close()
            conn.close
            #print("Connention to PosgresSQL is closed")