import psycopg2


def create_pool():
    try:
        postgresql_pool = psycopg2.pool.SimpleConnectionPool(1,20,
            host='127.0.0.1',
            port='5432',
            user='vitali',
            password='vit777pv',
            database='stdata' # создать базу данных stdata до первого использования
        )
        if postgresql_pool:
            print("Postgresql_pool created successfully")

    except(Exception, psycopg2.DatabaseError) as error:
        print("Error connecting PostgreSql", error)
    finally:
        return postgresql_pool
        



#---------------------------------------------------------------------------------------------


def delete_pool( postgresql_pool ):
    if postgresql_pool:
        postgresql_pool.closeall()
        print("Pool is closed")



#---------------------------------------------------------------------------------------------



def normel_write_database_st( postgresql_pool,
                                    number,
                                    u_in_f1,
                                    u_in_f2,
                                    u_in_f3,
                                    u_out_f1,
                                    u_out_f2,
                                    u_out_f3,
                                    i_in_f1,
                                    i_in_f2,
                                    i_in_f3,
                                    i_out_f1,
                                    i_out_f2,
                                    i_out_f3,
                                    p1,
                                    p2,
                                    p3,
                                    code_error ):
    try:
        #----getconn() gets connection from pool
        connection = postgresql_pool.getconn()
        cursor = connection.cursor()

        str_number = 'st_normel_' + str(number)    #имя таблицы
        
        # запись данных в бд st_normel_number
        insert_query = f"INSERT INTO {str_number}( u_in_f1,\
            u_in_f2,\
            u_in_f3,\
            u_out_f1,\
            u_out_f2,\
            u_out_f3,\
            i_in_f1,\
            i_in_f2,\
            i_in_f3,\
            i_out_f1,\
            i_out_f2,\
            i_out_f3,\
            p1,\
            p2,\
            p3,\
            code_error) VALUES ( {u_in_f1},\
            {u_in_f2},\
            {u_in_f3},\
            {u_out_f1},\
            {u_out_f2},\
            {u_out_f3},\
            {i_in_f1},\
            {i_in_f2},\
            {i_in_f3},\
            {i_out_f1},\
            {i_out_f2},\
            {i_out_f3},\
            {p1},\
            {p2},\
            {p3},\
            {code_error})"
        cursor.execute(insert_query)
        connection.commit()

    except(Exception, psycopg2.DatabaseError) as error:
        print("Error writing PostgreSql", error)
    finally:
        if connection:
            #----putconn() puts connection to pool
            cursor.close()
            postgresql_pool.putconn(connection)


#---------------------------------------------------------------------------------------------


def normel_create_database_list_addressip(postgresql_pool):
    try:
        #----getconn() gets connection from pool
        connection = postgresql_pool.getconn()
        cursor = connection.cursor()
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE IF NOT EXISTS normel_list_st(
            id BIGSERIAL NOT NULL PRIMARY KEY,
            number BIGINT NOT NULL,
            address TEXT,
            coordinates VARCHAR(50),
            numbersim VARCHAR(20),
            UNIQUE(number)
        ); '''
        cursor.execute(create_table_query)
        connection.commit()
        
        # SQL-запрос для создания новой таблицы
        create_table_query = '''CREATE TABLE IF NOT EXISTS normel_address_ip(
            id BIGSERIAL NOT NULL PRIMARY KEY,
            number BIGINT REFERENCES normel_list_st(number),
            ip_address VARCHAR(50),
            port_address INTEGER,
            time_create TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        ); '''
        cursor.execute(create_table_query)
        connection.commit()

    except(Exception, psycopg2.DatabaseError, psycopg2.Error) as error:
        print("Error writing PostgreSql", error)
    finally:
        if connection:
            #----putconn() puts connection to pool
            cursor.close()
            postgresql_pool.putconn(connection)


#---------------------------------------------------------------------------------------------

def normel_add_new_station(postgresql_pool, number):
    try:
        #----getconn() gets connection from pool
        connection = postgresql_pool.getconn()
        cursor = connection.cursor()

        str_number = 'st_normel_' + str(number)    #имя таблицы
        # Выполнение SQL-запроса для вставки данных в таблицу
        create_query = f"CREATE TABLE {str_number}(id BIGSERIAL NOT NULL PRIMARY KEY,\
            u_in_f1 REAL NOT NULL,\
            u_in_f2 REAL NOT NULL,\
            u_in_f3 REAL NOT NULL,\
            u_out_f1 REAL NOT NULL,\
            u_out_f2 REAL NOT NULL,\
            u_out_f3 REAL NOT NULL,\
            i_in_f1 REAL NOT NULL,\
            i_in_f2 REAL NOT NULL,\
            i_in_f3 REAL NOT NULL,\
            i_out_f1 REAL NOT NULL,\
            i_out_f2 REAL NOT NULL,\
            i_out_f3 REAL NOT NULL,\
            p1 REAL NOT NULL,\
            p2 REAL NOT NULL,\
            p3 REAL NOT NULL,\
            code_error INTEGER NOT NULL,\
            time_create TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP)"

        cursor.execute(create_query)
        connection.commit()
        
        # запись новой станции в бд normel_list_st
        insert_query = f"INSERT INTO normel_list_st(number) VALUES ({number})"
        cursor.execute(insert_query)
        connection.commit()
        
        # # запись новой станции в бд normel_address_ip
        # insert_query = f"INSERT INTO normel_address_ip(number) VALUES ({number})"
        # cursor.execute(insert_query)
        # connection.commit()

    except(Exception, psycopg2.DatabaseError, psycopg2.Error) as error:
        print("Error writing PostgreSql", error)
    finally:
        if connection:
            #----putconn() puts connection to pool
            cursor.close()
            postgresql_pool.putconn(connection)


#---------------------------------------------------------------------------------------------

def normel_write_database_address_ip(postgresql_pool, number, addr_recv):
    try:
        #----getconn() gets connection from pool
        connection = postgresql_pool.getconn()
        cursor = connection.cursor()
        
        # запись данных в бд normel_address_ip
        #insert_query = f"UPDATE normel_address_ip SET ip_address='{addr_recv[0]}', port_address='{addr_recv[1]}', time_create=NOW() WHERE number={number}"
        insert_query = f"INSERT INTO normel_address_ip(number, ip_address, port_address, time_create) VALUES ({number}, '{addr_recv[0]}', {addr_recv[1]}, NOW())"

        cursor.execute(insert_query)
        connection.commit()

    except(Exception, psycopg2.DatabaseError, psycopg2.Error) as error:
        print("Error writing PostgreSql", error)
    finally:
        if connection:
            #----putconn() puts connection to pool
            cursor.close()
            postgresql_pool.putconn(connection)
