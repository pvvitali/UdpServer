import socket
from SimplePoolNormel import *

# создать базу данных stdata до первого использования

#server data
host = "195.133.145.188"
port = 50003
addr = (host, port)
#
bufferSize = 1024

list_stations = []  #список станций
old_addr_recv = {}     #список значений ip и порта, полученных ранее в пакете от станции


try:
    postgresql_pool = create_pool() #создаем pool с базой данных
    normel_create_database_list_addressip(postgresql_pool) #создаем две таблицы если они еще не созданы
        
    #create socket
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    #bind
    #bind - связывает адрес и порт с сокетом
    udp_socket.bind(addr)

    print("UDP server up and listening(port=50003)...")

    while True:
        data_recv,addr_recv = udp_socket.recvfrom(bufferSize)
        #str.encode - перекодирует введенные строчные данные в байты, bytes.decode - обратно
        data_recv = bytes.decode(data_recv) 

        #разбор строки
        try:
            i=0; y=5
            number = int(data_recv[i:y])
            i+=6; y+=6
            u_in_f1 = float(data_recv[i:y])
            i+=6; y+=6
            u_in_f2 = float(data_recv[i:y])
            i+=6; y+=6
            u_in_f3 = float(data_recv[i:y])
            i+=6; y+=6
            u_out_f1 = float(data_recv[i:y])
            i+=6; y+=6
            u_out_f2 = float(data_recv[i:y])
            i+=6; y+=6
            u_out_f3 = float(data_recv[i:y])
            i+=6; y+=6
            i_in_f1 = float(data_recv[i:y])
            i+=6; y+=6
            i_in_f2 = float(data_recv[i:y])
            i+=6; y+=6
            i_in_f3 = float(data_recv[i:y])
            i+=6; y+=6
            i_out_f1 = float(data_recv[i:y])
            i+=6; y+=6
            i_out_f2 = float(data_recv[i:y])
            i+=6; y+=6
            i_out_f3 = float(data_recv[i:y])
            i+=6; y+=6
            p1 = float(data_recv[i:y])
            i+=6; y+=6
            p2 = float(data_recv[i:y])
            i+=6; y+=6
            p3 = float(data_recv[i:y])
            i+=6; y+=6
            code_error = int(data_recv[i:y])
        except(ValueError) as err:
            print("Error to convert str to int: ", err)
            continue    #ждем прихода следующей строки

        #ошибок разбора строки нет, проверим есть ли станция в списке
        if number not in list_stations:
            normel_add_new_station(postgresql_pool, number)    #add new station to database
            list_stations.append(number)                #add new station to list
            #
            old_addr_recv[number] = ['0', 0]    #add new address to dict
        normel_write_database_st(postgresql_pool, number,
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
                                                code_error )
        if addr_recv[0] != old_addr_recv[number][0] and addr_recv[1] != old_addr_recv[number][1] :
            normel_write_database_address_ip(postgresql_pool, number, addr_recv)
            old_addr_recv[number][0] = addr_recv[0]
            old_addr_recv[number][1] = addr_recv[1]
        
        

except(Exception) as error:
    print(error)

finally:
    delete_pool( postgresql_pool )
    if udp_socket:
        udp_socket.close

