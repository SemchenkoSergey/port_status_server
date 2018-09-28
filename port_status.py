#!/usr/bin/env python3
# coding: utf8

import datetime
import time
import MySQLdb
import os
import pickle
import subprocess
from resources import Settings
from resources import DslamHuawei
from resources import SQL
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings("ignore")

DslamHuawei.LOGGING = True


def load_dslams():
    try:
        with open('resources{}dslams.db'.format(os.sep), 'br') as file_load:
            return pickle.load(file_load)
    except:
        return []
    
def connect_dslam(host):
    #Создание объекта dslam
    ip = host[0]
    model = host[1]
    DEVNULL = os.open(os.devnull, os.O_WRONLY)
    response = subprocess.call('ping -c 3 {}'.format(ip), shell='True', stdout=DEVNULL, stderr=subprocess.STDOUT)
    if response != 0:
        print('{} не доступен'.format(ip))
        return None
    if model == '5600':
        try:
            dslam = DslamHuawei.DslamHuawei5600(ip, Settings.login, Settings.password, 20)
        except:
            print('{} не удалось подключиться'.format(ip))
            return None
    elif model == '5616':
        try:
            dslam = DslamHuawei.DslamHuawei5616(ip, Settings.login, Settings.password, 20)
        except:
            print('{} не удалось подключиться'.format(ip))
            return None
    else:
        return None
    return dslam

def run(host):    
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    
    dslam = connect_dslam(host)
    if dslam is None:
        return (0, host)
    hostname = dslam.get_info()['hostname']
    ip = dslam.get_info()['ip']
    for board in dslam.boards:
        current_time = datetime.datetime.now()
        paramConnectBoard = dslam.get_line_operation_board(board)
        if paramConnectBoard == False:
            continue
        for port in range(0,dslam.ports):
            param = paramConnectBoard[port]
            if param['up_snr'] == '-':
                options = {'cursor': cursor,
                           'table_name': 'data_dsl',
                           'str1': 'hostname, board, port, datetime',
                           'str2': '"{}", {}, {}, "{}"'.format(hostname, board, port, current_time.strftime('%Y-%m-%d %H:%M:%S'))}
                SQL.insert_table(**options)
                continue
            options = {'cursor': cursor,
                       'table_name': 'data_dsl',
                       'str1': 'hostname, board, port, up_snr, dw_snr, up_att, dw_att, max_up_rate, max_dw_rate, up_rate, dw_rate, datetime',
                       'str2': '"{}", {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, "{}"'.format(hostname, board, port, param['up_snr'], param['dw_snr'], param['up_att'], param['dw_att'], param['max_up_rate'], param['max_dw_rate'], param['up_rate'], param['dw_rate'], current_time.strftime('%Y-%m-%d %H:%M:%S'))}
            SQL.insert_table(**options)
    connect.close()
    del dslam
    return (1, host)


def main():
    print('Время запуска: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))
    # Загрузка списка DSLAM
    dslams = load_dslams()
    if len(dslams) == 0:
        print('Не найден dslams.db!')
        return    
    dslam_ok = 0
    dslam_repeat = []
    dslam_bad = []
    # Создание таблицы(если еще нет)
    SQL.create_data_dsl()
    # Запуск основного кода
    #current_time = datetime.datetime.now()
    #arguments = [(current_time, host) for host in dslams]
    with ThreadPoolExecutor(max_workers=Settings.threads) as executor:
        results = executor.map(run, dslams)
    
    for result in results:
        if result is None:
            continue
        elif result[0] == 1:
            dslam_ok += 1
        else:
            dslam_repeat.append(result[1])
    if len(dslam_repeat) > 0:
        print('Пауза 5 мин, и повторная обработка DSLAM:')
        for dslam in dslam_repeat:
            print(dslam[0])
        print()
        # Задержка
        time.sleep(60 * 5)
        
        # Повторная обработка DSLAM
        with ThreadPoolExecutor(max_workers=Settings.threads) as executor:
            results = executor.map(run, dslam_repeat)
        
        for result in results:
            if result is None:
                continue
            elif result[0] == 1:
                dslam_ok += 1
            else:
                dslam_bad.append(result[1][0])    
    
    # Распечатка результатов
    print('Время окончания: {}'.format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))
    print('Всего DSLAM: {}'.format(len(dslams)))
    print('Обработано: {}'.format(dslam_ok))
    print('Необработанные: {}'.format(', '.join(dslam_bad)))
    print('---------\n')
            
    # Удаление старых записей
    options = {'table_name': 'data_dsl',
               'str1': 'CAST(datetime AS DATE) < DATE_ADD(CURRENT_DATE(), INTERVAL -{} DAY)'.format(Settings.days)}
    SQL.delete_table(**options)



if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)
    main()