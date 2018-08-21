#!/usr/bin/env python3
# coding: utf8

import os
import re
import csv
import datetime
import MySQLdb
from resources import Onyma
from resources import SQL
from resources import Settings
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings("ignore")

argus_phone = {}        # 'id из аргуса': 'номер телефона'
phones = {}             # {'телефон': [(account, onyma_id),...]}

def get_area_code(area):
    codes = (('БЛАГОДАРНЕНСКИЙ', 'Благодарный', '86549'),
             ('БУДЕННОВСКИЙ', 'Буденновск', '86559'),
             ('ГЕОРГИЕВСКИЙ', 'Георгиевск', '87951'),
             ('СОВЕТСКИЙ', 'Зеленокумск', '86552'),
             ('ИЗОБИЛЬНЕНСКИЙ', 'Изобильный', '86545'),
             ('ИПАТОВСКИЙ', 'Ипатово', '86542'),
             ('МИНЕРАЛОВОДСКИЙ', 'Минеральные Воды', '87922'),
             ('ШПАКОВСКИЙ', 'Михайловск', '86553'),
             ('НЕФТЕКУМСКИЙ', 'Нефтекумск', '86558'),
             ('НОВОАЛЕКСАНДРОВСКИЙ', 'Новоалександровск', '86544'),
             ('КИРОВСКИЙ', 'Новопавловск', '87938'),
             ('ПЕТРОВСКИЙ', 'Светлоград', '86547'),
             ('АЛЕКСАНДРОВСКИЙ', 'Александровское', '86557'),
             ('АРЗГИРСКИЙ', 'Арзгир', '86560'),
             ('ГРАЧЕВСКИЙ', 'Грачевка', '86540'),
             ('АПАНАСЕНКОВСКИЙ', 'Дивное', '86555'),
             ('ТРУНОВСКИЙ', 'Донское', '86546'),
             ('КОЧУБЕЕВСКИЙ', 'Кочубеевское', '86550'),
             ('КРАСНОГВАРДЕЙСКИЙ', 'Красногвардейское', '86541'),
             ('АНДРОПОВСКИЙ', 'Курсавка', '86556'),
             ('ЛЕВОКУМСКИЙ', 'Левокумское', '86543'),
             ('ТУРКМЕНСКИЙ', 'Летняя Ставка', '86565'),
             ('НОВОСЕЛИЦКИЙ', 'Новоселицкое', '86548'),
             ('СТЕПНОВСКИЙ', 'Степное', '86563'),
             ('ПРЕДГОРНЫЙ', 'Ессентукская', '87961'),
             ('КУРСКИЙ', 'Курская', '87964'),
             ('Ессентуки', 'Ессентуки', '87934'),
             ('Железноводск', 'Железноводск', '87932'),
             ('Кисловодск', 'Кисловодск', '87937'),
             ('Лермонтов', 'Лермонтов', '87935'),
             ('Невинномысск', 'Невинномысск', '86554'),
             ('Пятигорск', 'Пятигорск', '8793'),
             ('Ставрополь', 'Ставрополь', '8652'))
    for code in codes:
        if (code[0].lower() in area.lower()) or (code[1].lower() in area.lower()):
            return code[2]
    return False


def run_define_param(account_list):
    count_processed = 0
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    onyma = Onyma.get_onyma()
    if onyma is None:
        return count_processed
    
    for account in account_list:
        account_name = account[0]
        account_param = Onyma.find_account_param(onyma, account_name)
        if account_param is False:
            continue
        elif account_param == -1:
            onyma = Onyma.get_onyma()
            if onyma is None:
                return count_processed            
            continue
        else:
            bill, dmid, tmid = account_param
        count_processed += 1
        options = {'cursor': cursor,
                   'table_name': 'abon_onyma',
                   'str1': 'account_name, bill, dmid, tmid',
                   'str2': '"{}", "{}", "{}", "{}"'.format(account_name, bill, dmid, tmid)}        
        SQL.insert_table(**options)
    connect.close()
    del onyma
    return count_processed


def run_define_speed(account_list):
    count_processed = 0
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    onyma = Onyma.get_onyma()
    if onyma is None:
        return count_processed     
    
    for account in account_list:
        account_name = account[0]
        speed = Onyma.find_account_speed(onyma, account_name)
        if speed is not False:
            options = {'cursor': cursor,
                       'table_name': 'abon_dsl',
                       'str1': 'tariff = {}'.format(speed),
                       'str2': 'account_name = "{}"'.format(account_name)}
            SQL.update_table(**options)
            count_processed += 1
    connect.close()
    del onyma
    return count_processed


def find_phone_account(accounts): 
    result = []
    onyma = Onyma.get_onyma()
    if onyma is None:
        return None
    for account in accounts:
        #print('Поиск {} в Ониме'.format(account[0]))
        argus_id = Onyma.find_argus_id(onyma, account[1])
        if argus_id in argus_phone:
            result.append((account[0], argus_phone[argus_id]))
            #print('Учетное имя - {}, телефон - {}'.format(result[-1][0], result[-1][1]))
    return result # [(account_name, phone_number), ...]


def argus_files(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()     
    
    # Подготовка регулярных выражений
    re_phone = re.compile(r'\((\d+)\)(.+)') # Код, телефон
    re_address = re.compile(r'(.*),\s?(.*),\s?(.*),\s?(.*),\s?кв\.(.*)') # Район, нас. пункт, улица, дом, кв.
    re_board = re.compile(r'.+0.(\d+)') # Board
    re_onyma = re.compile(r'.+Onyma\s*(\d+)') # Onyma id
    
    # Обработка csv-файлов
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if len(row) < 8:
                    continue
                cell_hostname = row[2].replace('=', '').replace('"', '')
                cell_board = row[4].replace('=', '').replace('"', '')
                cell_port = row[5].replace('=', '').replace('"', '')
                cell_phone = row[8].replace('=', '').replace('"', '')
                cell_address = row[10].replace('=', '').replace('"', '')
                cell_onyma = row[12].replace('=', '').replace('"', '')
                cell_type = row[7].replace('=', '').replace('"', '')
                if cell_type not in ('Телефон', 'Прямой провод') or not re_phone.search(cell_phone) or not re_address.search(cell_address):
                            continue
                
                hostname = '"{}"'.format(cell_hostname)                                     # hostname
                board = re_board.search(cell_board).group(1)                                # board
                port = cell_port                                                            # port
                area_code = re_phone.search(cell_phone).group(1)                            # код телефона
                phone = re_phone.search(cell_phone).group(2)                                # телефон
                phone_number = '"{}{}"'.format(area_code, phone).replace('ПППП', 'ПП')      # полный номер (код+телефон)
                area = '"{}"'.format(re_address.search(cell_address).group(1))              # район
                locality = '"{}"'.format(re_address.search(cell_address).group(2))          # нас. пункт
                street = '"{}"'.format(re_address.search(cell_address).group(3))            # улица
                house_number = '"{}"'.format(re_address.search(cell_address).group(4))      # номер дома
                apartment_number = '"{}"'.format(re_address.search(cell_address).group(5))  # квартира
                try:
                    onyma_equ = re_onyma.search(cell_onyma).group(1)                        # onyma equ
                except:
                    onyma_equ = ''
                    
                # Вставка данных в таблицу
                options = {'cursor': cursor,
                           'table_name': 'abon_dsl',
                           'str1': 'phone_number, area, locality, street, house_number, apartment_number, hostname, board, port',
                           'str2': '{}, {}, {}, {}, {}, {}, {}, {}, {}'.format(phone_number, area, locality, street, house_number, apartment_number, hostname, board, port)}
                try:
                    SQL.insert_table(**options)
                except:
                    continue
                argus_phone[onyma_equ] = phone_number
    connect.close()
 
def onyma_file(file_list):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    onyma = True
    
    for file in file_list:
        if file.split('.')[-1] != 'csv':
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader(f, delimiter=';')                    
            for row in reader:
                if (row[41] != 'deleted') and (re.search(r'[xA]DSL', row[37])) and (row[23] == 'SSG-подключение'):
                    area_code = get_area_code(row[1])
                    if area_code is False:
                        continue
                    if (len(row[7]) == 10) or (area_code in row[7]):
                        phone_number = '"{}"'.format(row[7])
                    elif (len(row[7]) < 10) and (len(row[7]) > 0):
                        phone_number = '"{}{}"'.format(area_code, row[7]).replace('-', '')
                    else:
                        continue
                    
                    # Определение учетного имени
                    account_name = '"{}"'.format(row[21])
                    onyma_id = row[19]
                    if phone_number not in phones:
                        phones[phone_number] = []
                    phones[phone_number].append((account_name, onyma_id))
                  
            for phone in phones:
                if len(phones[phone]) == 1:
                    options = {'cursor': cursor,
                               'table_name': 'abon_dsl',
                               'str1': 'account_name = {}'.format(phones[phone][0][0]),
                               'str2': 'phone_number = {}'.format(phone)}                    
                    SQL.update_table(**options)
                else:
                    if onyma is True:
                        find_phones = find_phone_account(phones[phone])
                        if find_phones is None:
                            onyma = False
                            continue
                    else:
                        continue
                    for find_phone in find_phones:
                        options = {'cursor': cursor,
                                   'table_name': 'abon_dsl',
                                   'str1': 'account_name = {}'.format(find_phone[0]),
                                   'str2': 'phone_number = {}'.format(find_phone[1])}
                        SQL.update_table(**options)
    connect.close()
    
def delete_files(file_list):
    for file in file_list:
        os.remove(file)

    
def main():
    # Просмотр файлов в директории in/argus/
    argus_file_list = ['in' + os.sep + 'argus' + os.sep + x for x in os.listdir('in' + os.sep + 'argus')]
    
    # Просмотр файлов в директории in/onyma/
    onyma_file_list = ['in' + os.sep + 'onyma' + os.sep + x for x in os.listdir('in' + os.sep + 'onyma')]  
    
    if len(argus_file_list) == 0 or len(onyma_file_list) == 0:
        return
    
    print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    SQL.create_abon_dsl(drop=True)
    SQL.create_abon_onyma(drop=True)    
    
    # Обработка файлов в директории in/argus/
    argus_files(argus_file_list)
    delete_files(argus_file_list)
    
    # Обработка файлов в директории in/onyma/
    onyma_file(onyma_file_list)
    delete_files(onyma_file_list)
    
    # Заполнение полей bill, dmid, tmid таблицы abon_onyma
    options = {'table_name': 'abon_dsl',
               'str1': 'account_name',
               'str2': 'account_name IS NOT NULL'}
    account_list = SQL.get_table_data(**options)
    if len(account_list) == 0:
        print('\n!!! Не сформирована таблица abon_dsl !!!\n')
        return
    
    arguments = [account_list[x::Settings.threads_count]  for x in range(0,  Settings.threads_count)]
    print('Получение данных из Онимы для таблицы abon_onyma...')
    with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
        result = executor.map(run_define_param, arguments)
    count = 0
    for i in result:
        count += i
    print('Обработано: {}'.format(count))
    
    # Заполнение тарифов в abon_dsl
    print('Получение данных из Онимы для заполнения тарифов...')
    with ThreadPoolExecutor(max_workers=Settings.threads_count) as executor:
        result = executor.map(run_define_speed, arguments)
    count = 0
    for i in result:
        count += i
    print('Обработано: {}'.format(count))    
    print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('---------\n')
    
    
if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)
    main()