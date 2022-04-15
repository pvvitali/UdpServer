import socket
from SimplePool import *

# создать базу данных stdata до первого использования

#server data
host = "195.133.145.188"
port = 50002
addr = (host, port)
#
bufferSize = 1024

list_stations = []  #список станций
old_addr_recv = ('0.0.0.0', 0) #кортеж значений ip и порта, полученных ранее в пакете от станции


try:
    postgresql_pool = create_pool() #создаем pool с базой данных
    create_database_list_addressip(postgresql_pool) #создаем две таблицы если они еще не созданы
        
    #create socket
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    #bind
    #bind - связывает адрес и порт с сокетом
    udp_socket.bind(addr)

    print("UDP server up and listening...")

    while True:
        data_recv,addr_recv = udp_socket.recvfrom(bufferSize)
        #str.encode - перекодирует введенные строчные данные в байты, bytes.decode - обратно
        data_recv = bytes.decode(data_recv) 

        #разбор строки
        try:
            number = int(data_recv[0:5])
            u = float(data_recv[9:13])
            i = float(data_recv[17:22])
            p1 = float(data_recv[25:30])
            p2 = float(data_recv[34:39])
        except(ValueError) as err:
            print("Error to convert str to int: ", err)
            continue    #ждем прихода следующей строки

        #ошибок разбора строки нет, проверим есть ли станция в списке
        if number not in list_stations:
            add_new_station(postgresql_pool, number)    #add new station to database
            list_stations.append(number)                #add new station to list
        write_database_st(postgresql_pool, number, u, i, p1, p2)
        if addr_recv[0] != old_addr_recv[0] and addr_recv[1] != old_addr_recv[1] :
            write_database_address_ip(postgresql_pool, number, addr_recv)
            old_addr_recv[0] = addr_recv[0]
            old_addr_recv[1] = addr_recv[1]
        
        

except(Exception) as error:
    print(error)

finally:
    delete_pool( postgresql_pool )
    if udp_socket:
        udp_socket.close

