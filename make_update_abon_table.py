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


class Session():
    #
    # Класс для работы с сессиями
    #
    def __init__(self, account_name, hostname, board, port, mac_address, dtime):
        self.account_name = account_name
        self.hostname = hostname
        self.board = int(board)
        self.port = int(port)
        self.mac_address = mac_address
        self.dtime = dtime
        
    def __str__(self):
        return '{}({}):\n{}\n{}\n{}\n'.format(self.account_name, self.hostname, self.board, self.port, self.mac_address)
        
    def __gt__(self, other):
        return self.dtime > other.dtime
    
    def __lt__(self, other):
        return self.dtime < other.dtime


def delete_files(files):
    #
    # Функция удаления файлов по списку
    #
    for file in files:
        os.remove(file)


def get_area_code(area):
    #
    # Определение телефонного кода города
    #
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


def parsing_make_abon_onyma(file_list):
    #
    # Функция обработки файла 'Список подключений ШПД + ТВ' и занесения данных в базу
    #
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    phones = {}             # {'телефон': {'accounts': [], 'count': кол-во}}
    tv = []                 # Список телефонов с IPTV   
    # Чтение информации из файлов
    for file in file_list:
        if (file.split('.')[-1] != 'csv') or ('Список подключений ШПД + ТВ' not in file):
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')                    
            for row in reader:
                if (row[41] != 'deleted') and (re.search(r'[xA]DSL', row[36])):
                    area_code = get_area_code(row[1])
                    if area_code is False:
                        continue
                    if (len(row[7]) == 10) and (area_code in row[7]):
                        phone_number = '"{}"'.format(row[7])
                    elif (len(row[7]) < 10) and (len(row[7]) > 0):
                        phone_number = '"{}{}"'.format(area_code, row[7]).replace('-', '')
                    else:
                        phone_number = '"{}"'.format(row[7])
                    
                    if row[23] == 'SSG-подключение':
                        # Определение учетного имени
                        account_name = '"{}"'.format(row[21])
                        if phone_number not in phones:
                            phones[phone_number] = []
                        phones[phone_number].append({'account_name': account_name, 'tariff': '"{}"'.format(row[26].replace('"', "'")), 'address': '"{}"'.format(row[6].replace('"', "'")), 'servis_point': '"{}"'.format(row[1]), 'contract': '"{}"'.format(row[3])})
                    elif row[23] == '[ЮТК] Сервис IPTV':
                        tv.append(phone_number)
            # Удаляю обработанный файл (так как нужен список, передаю список)
            delete_files([file])           
    # Занесение в базу данных
    for phone_numbers in phones:
        for account in phones[phone_numbers]:
            servis_point = account['servis_point']
            contract = account['contract']
            account_name = account['account_name']
            tariff = account['tariff']
            address = account['address']
            if len(phone_numbers) == 1:
                options = {'cursor': cursor,
                           'table_name': 'abon_onyma',
                           'str1': 'account_name, phone_number, contract, servis_point, address, tariff',
                           'str2': '{}, {}, {}, {}, {}, {}'.format(account_name, phone_numbers, contract, servis_point, address, tariff)}
            else:
                options = {'cursor': cursor,
                           'table_name': 'abon_onyma',
                           'str1': 'account_name, contract, servis_point, address, tariff',
                           'str2': '{}, {}, {}, {}, {}'.format(account_name, contract, servis_point, address, tariff)}                
            try:
                SQL.insert_table(**options)
            except:
                continue                
        if phone_numbers in tv:
            options = {'cursor': cursor,
                       'table_name': 'abon_onyma',
                       'str1': 'tv = "yes"',
                       'str2': 'phone_number = {}'.format(phone_numbers)}
            SQL.update_table(**options)
    connect.close()


def parsing_update_abon_onyma(files):
    #
    # Функция обработки файлов с сессиями
    # возвращает словарь вида {'account_name': {'hostname': '', 'board': '', 'port': '',  'mac_address': '', 'dtime': ''}}
    #
    try:
        with open('resources{}session_files.db'.format(os.sep), 'br') as file_load:
            session_files = pickle.load(file_load)
    except:
        session_files = []    
    sessions = {}
    re_dslam =  re.compile(r'ST:\s+(.+?) atm 0/(\d+)/0/(\d+)')
    re_mac_address = re.compile(r'MAC:\s+(.+?)\s')
    for file in files:
        if (file.split('.')[-1] != 'csv') or ('Абонентские сессии' not in file):
            continue
        if file in session_files:
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader((line.replace('\0','') for line in f), delimiter=';')
            for row in reader:
                if row[11] != 'DSL' or not re_dslam.search(row[6]):
                    continue
                account_name = row[5]
                hostname = re_dslam.search(row[6]).group(1)
                board = re_dslam.search(row[6]).group(2)
                port = re_dslam.search(row[6]).group(3)
                mac_address = re_mac_address.search(row[6]).group(1).replace('.', '')
                dtime =  datetime.datetime.strptime(row[0], '%d.%m.%Y %H:%M:%S')
                session = Session(account_name, hostname, board, port, mac_address, dtime)
                if account_name in sessions:
                    if session > sessions[account_name]:
                        sessions[account_name] = session
                else:
                    sessions[account_name] = session
        session_files.append(file)
    with open('resources{}session_files.db'.format(os.sep), 'bw') as file_dump:
            pickle.dump(session_files, file_dump)
    return sessions

def get_current_ports():
    #
    # Функция получения текущих значений портов для учетных имен
    # возвращает словарь вида {'account_name': {'hostname': '', 'board': '', 'port': ''}}
    #
    result = {}
    # Получение данных из базы данных
    options = {'table_name': 'abon_onyma',
               'str1': 'account_name, hostname, board, port',
               'str2': 'account_name IS NOT NULL'}
    accounts = SQL.get_table_data(**options)
    for account in accounts:
        result[account[0]] = {'hostname': account[1], 'board': account[2], 'port': account[3]}
    return result


def update_abon_onyma(file_list):
    #
    # Функция запуска обработки файлов с сессиями и обновления данных о портах
    #
    sessions = parsing_update_abon_onyma(file_list)
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    current_ports = get_current_ports()
    for session in sessions:
        if session not in current_ports:
            continue
        if (sessions[session].hostname != current_ports[session]['hostname']) or (sessions[session].board != current_ports[session]['board']) or (sessions[session].port != current_ports[session]['port']):
            options = {'cursor': cursor,
                       'table_name': 'abon_onyma',
                       'str1': 'hostname = "{}", board = {}, port = {}, mac_address = "{}", datetime = "{}"'.format(sessions[session].hostname, sessions[session].board, sessions[session].port, sessions[session].mac_address, sessions[session].dtime.strftime('%Y-%m-%d %H:%M:%S')),
                       'str2': 'account_name = "{}"'.format(session)}
            SQL.update_table(**options)
            print('{}: {}/{}/{} --> {}/{}/{}'.format(session, current_ports[session]['hostname'], current_ports[session]['board'], current_ports[session]['port'], sessions[session].hostname, sessions[session].board, sessions[session].port))
    connect.close()


def parsing_make_abon_argus(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    models = ['Huawei MA 5616', 'Huawei MA 5600']
    
    # Подготовка регулярного выражения
    re_phone = re.compile(r'\((\d+)\)(.+)')                                 # Код, телефон
    
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
                cell_phone = row[9].replace('=', '').replace('"', '')
                cell_address = row[6].replace('=', '').replace('"', '')
                if not re_phone.search(cell_phone) or cell_address == '':
                    continue
                try:
                    area_code = re_phone.search(cell_phone).group(1)                                                                                # код телефона
                    phone = re_phone.search(cell_phone).group(2)                                                                                    # телефон
                    phone_number = '"{}{}"'.format(area_code, phone).replace('ПППП', 'ПП')                                                          # полный номер (код+телефон)
                    if re.search(r'.*р-н', cell_address):                                                                                           # в адресе есть район
                        area = '"{}"'.format(re.search(r'.*р-н', cell_address).group(0))                                                            # район
                        locality = '"{}"'.format(re.search(r'р-н, (.*?),', cell_address).group(1))                                                  # нас. пункт                    
                    elif re.search(r'.+\sг\.,\s+(.+\s(?:п|г|с|х|ст-ца|аул)?\.?),', cell_address):                                                   # в адресе есть город, затем еще город, село, поселок, хутор и т.д.
                        area = '"{}"'.format(re.search(r'^(.+\sг\.),', cell_address).group(1))                                                      # район
                        locality = '"{}"'.format(re.search(r'.+\sг\.,\s+(.+\s(?:п|г|с|х|ст-ца|аул)?\.?),', cell_address).group(1))                  # нас. пункт
                    elif re.search(r'^(.+\sг\.),', cell_address):                                                                                   # адрес начинается с города
                        area = '"{}"'.format(re.search(r'^(.+\sг\.),', cell_address).group(1))                                                      # район
                        locality = area                                                                                                             # нас. пункт
                    street = '"{}"'.format(re.search(r'(?:.+(?:п|г|с|х|ст-ца|аул|аул)?\.?),\s+(.+?),\s+(?:.+),\s?кв\.', cell_address).group(1))     # улица
                    house_number = '"{}"'.format(re.search(r'(\S+?)\s*,кв', cell_address).group(1))                                                 # дом
                    apartment_number = '"{}"'.format(re.search(r'кв.\s?(.*)', cell_address).group(1))                                               # квартира
                except Exception as ex:
                    #print('-------------------------------')
                    #print(ex)
                    #print(cell_address)
                    #print(cell_phone)
                    continue
                    
                #print( '{}, {}, {}, {}, {}, {}, {}, {}, {}'.format(phone_number, area, locality, street, house_number, apartment_number))
                ## Вставка данных в таблицу
                if len(phone_number) > 12:
                    continue
                options = {'cursor': cursor,
                           'table_name': 'abon_argus',
                           'str1': 'phone_number, area, locality, street, house_number, apartment_number',
                           'str2': '{}, {}, {}, {}, {}, {}'.format(phone_number, area, locality, street, house_number, apartment_number)}
                try:
                    SQL.insert_table(**options)
                except:
                    continue
    connect.close()


def make_abon_onyma(file_list):
    #
    # Функция формирования таблицы abon_onyma
    #    
    # Пересоздаем таблицу
    SQL.create_abon_onyma(drop=True)
    # Обнуляем список файлов с сессиями
    with open('resources{}session_files.db'.format(os.sep), 'bw') as file_dump:
        pickle.dump([], file_dump)
    # Заполнение таблицы из отчета Онимы 'Список подключений ШПД + ТВ'
    parsing_make_abon_onyma(file_list)
    # Заполнение данных о портах из отчета Онимы 'Абонентские сессии'
    update_abon_onyma(file_list)
    
    
def make_abon_argus(file_list):
    #
    # Функция формирования таблицы abon_argus
    #
    # Пересоздаем таблицу
    SQL.create_abon_argus(drop=True)
    parsing_make_abon_argus(file_list)
    delete_files(file_list)
    pass
    


def main():
    #
    # Запуск программы
    #
    print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    # Просмотр файлов в директории input/onyma/
    onyma_file_list = ['input' + os.sep + 'onyma' + os.sep + x for x in os.listdir('input' + os.sep + 'onyma')]
    # Проверка наличия файлов для abon_onyma
    abon_onyma_1 = False
    abon_onyma_2 = False
    for file in onyma_file_list:
        if 'Абонентские сессии' in file:
            abon_onyma_1 = True
        if 'Список подключений ШПД + ТВ' in file:
            abon_onyma_2 = True
    # Формирование таблицы abon_onyma
    if abon_onyma_1 and abon_onyma_2:
        print('Запуск формирования таблицы abon_onyma')
        make_abon_onyma(onyma_file_list)
    elif abon_onyma_1:
        # Найдены только файлы с сессиями, запускаем обновление таблицы
        print('Запуск процесса обновления информации о портах')
        update_abon_onyma(onyma_file_list)
    # Просмотр файлов в директории input/make_table/argus/
    argus_file_list = ['input' + os.sep + 'argus' + os.sep + x for x in os.listdir('input' + os.sep + 'argus')]
    if len(argus_file_list) > 0:
        make_abon_argus(argus_file_list)  
    print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('---------\n')
            
    
if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)
    main()