#!/usr/bin/env python3
# coding: utf8

import os
import re
import csv
import datetime
import MySQLdb
import pickle
from resources import SQL
from resources import Settings
import warnings
warnings.filterwarnings("ignore")


def get_area_code(area):
    codes = (('Благодарный', '86549'),
             ('Буденновск', '86559'),
             ('Георгиевск', '87951'),
             ('Зеленокумск', '86552'),
             ('Изобильный', '86545'),
             ('Ипатово', '86542'),
             ('Минеральные Воды', '87922'),
             ('Михайловск', '86553'),
             ('Нефтекумск', '86558'),
             ('Новоалександровск', '86544'),
             ('Новопавловск', '87938'),
             ('Светлоград', '86547'),
             ('Александровское', '86557'),
             ('Арзгир', '86560'),
             ('Грачевка', '86540'),
             ('Дивное', '86555'),
             ('Донское', '86546'),
             ('Кочубеевское', '86550'),
             ('Красногвардейское', '86541'),
             ('Курсавка', '86556'),
             ('Левокумское', '86543'),
             ('Летняя Ставка', '86565'),
             ('Новоселицкое', '86548'),
             ('Степное', '86563'),
             ('Ессентукская', '87961'),
             ('Курская', '87964'),
             ('Ессентуки', '87934'),
             ('Железноводск', '87932'),
             ('Кисловодск', '87937'),
             ('Лермонтов', '87935'),
             ('Невинномысск', '86554'),
             ('Пятигорск', '8793'),
             ('Ставрополь', '8652'))
    for code in codes:
        if (code[0].lower() in area.lower()):
            return code[1]
    return False


def argus_files(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    models = ['Huawei MA 5616', 'Huawei MA 5600']
    
    # Подготовка регулярных выражений
    re_phone = re.compile(r'\((\d+)\)(.+)')                                 # Код, телефон
    re_address = re.compile(r'(.*),\s?(.*),\s?(.*),\s?(.*),\s?кв\.(.*)')    # Район, нас. пункт, улица, дом, кв.
    re_board = re.compile(r'- (\d*\D)?(\d+)')                               # Board
    
    # Обработка csv-файлов
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if len(row) < 10:
                    continue
                cell_model = row[1].replace('=', '').replace('"', '')
                if cell_model not in models or not re.search(r'ADSL.+\(Л\)', row[4]):
                    continue
                cell_hostname = row[2].replace('=', '').replace('"', '')
                cell_board = row[4].replace('=', '').replace('"', '')
                cell_port = row[5].replace('=', '').replace('"', '')
                cell_phone = row[9].replace('=', '').replace('"', '')
                cell_address = row[6].replace('=', '').replace('"', '')
                cell_type = row[8].replace('=', '').replace('"', '')
                if not re_phone.search(cell_phone) or not re_address.search(cell_address):
                    continue
                
                try:
                    hostname = '"{}"'.format(cell_hostname)                                     # hostname
                    board = int(re_board.search(cell_board).group(2))                           # board
                    port = int(cell_port)                                                       # port
                    area_code = re_phone.search(cell_phone).group(1)                            # код телефона
                    phone = re_phone.search(cell_phone).group(2)                                # телефон
                    phone_number = '"{}{}"'.format(area_code, phone).replace('ПППП', 'ПП')      # полный номер (код+телефон)
                    area = '"{}"'.format(re_address.search(cell_address).group(1))              # район
                    locality = '"{}"'.format(re_address.search(cell_address).group(2))          # нас. пункт
                    street = '"{}"'.format(re_address.search(cell_address).group(3))            # улица
                    house_number = '"{}"'.format(re_address.search(cell_address).group(4))      # номер дома
                    apartment_number = '"{}"'.format(re_address.search(cell_address).group(5))  # квартира
                except:
                    print('except: {}'.format(cell_board))
                    continue
                    
                # Вставка данных в таблицу
                if len(phone_number) > 12:
                    continue
                options = {'cursor': cursor,
                           'table_name': 'abon_dsl',
                           'str1': 'phone_number, area, locality, street, house_number, apartment_number, hostname, board, port',
                           'str2': '{}, {}, {}, {}, {}, {}, {}, {}, {}'.format(phone_number, area, locality, street, house_number, apartment_number, hostname, board, port)}
                try:
                    SQL.insert_table(**options)
                except:
                    continue
    connect.close()
 
def onyma_file(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    phones = {}             # {'телефон': {'account_name': '', 'count': кол-во}}
    tv = []                 # Список телефонов с IPTV   
    # Чтение информации из файлов
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')                    
            for row in reader:
                if (row[41] != 'deleted') and (re.search(r'[xA]DSL', row[37])):
                    area_code = get_area_code(row[1])
                    if area_code is False:
                        continue
                    if (len(row[7]) == 10) or (area_code in row[7]):
                        phone_number = '"{}"'.format(row[7])
                    elif (len(row[7]) < 10) and (len(row[7]) > 0):
                        phone_number = '"{}{}"'.format(area_code, row[7]).replace('-', '')
                    else:
                        continue
                    
                    if row[23] == 'SSG-подключение':
                        # Определение учетного имени
                        account_name = '"{}"'.format(row[21])
                        if phone_number not in phones:
                            phones[phone_number] = {}
                            phones[phone_number]['count'] = 1
                        else:
                            phones[phone_number]['count'] += 1
                        phones[phone_number]['account_name'] = account_name
                    elif row[23] == '[ЮТК] Сервис IPTV':
                        tv.append(phone_number)
    # Занесение в базу данных
    for phone_number in phones:
        if phones[phone_number]['count'] == 1:
            #print('{}: {}, {}'.format(phone_number, phones[phone_number]['account_name'], phones[phone_number]['count']))
            options = {'cursor': cursor,
                       'table_name': 'abon_dsl',
                       'str1': 'account_name = {}'.format(phones[phone_number]['account_name']),
                       'str2': 'phone_number = {}'.format(phone_number)}                    
            SQL.update_table(**options)
            if phone_number in tv:
                options = {'cursor': cursor,
                           'table_name': 'abon_dsl',
                           'str1': 'tv = "yes"',
                           'str2': 'phone_number = {}'.format(phone_number)}
                SQL.update_table(**options)                               
        else:
            continue
    connect.close()
    
def delete_files(files):
    for file in files:
        os.remove(file)

    
def main():
    # Просмотр файлов в директории input/make_table/argus/
    argus_file_list = ['input' + os.sep + 'make_table' + os.sep + 'argus' + os.sep + x for x in os.listdir('input' + os.sep + 'make_table' + os.sep + 'argus')]
    
    # Просмотр файлов в директории input/make_table/onyma/
    onyma_file_list = ['input' + os.sep + 'make_table' + os.sep + 'onyma' + os.sep + x for x in os.listdir('input' + os.sep + 'make_table' + os.sep + 'onyma')]  
    
    if len(argus_file_list) == 0 or len(onyma_file_list) == 0:
        return
    
    print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    SQL.create_abon_dsl(drop=True) 
    
    # Обработка файлов в директории in/argus/
    argus_files(argus_file_list)
    delete_files(argus_file_list)
    
    # Обработка файлов в директории in/onyma/
    onyma_file(onyma_file_list)
    delete_files(onyma_file_list)
    
    with open('resources{}session_files.db'.format(os.sep), 'bw') as file_dump:
            pickle.dump([], file_dump)
    print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('---------\n')
    
    
if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)
    main()