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
    def __init__(self, account_name, hostname, board, port, dtime):
        self.account_name = account_name
        self.hostname = hostname
        self.board = int(board)
        self.port = int(port)
        self.dtime = dtime
        
    def __str__(self):
        return '{}:\n{}\n{}\n{}\n'.format(self.account_name, self.hostname, self.board, self.port)
        
    def __gt__(self, other):
        return self.dtime > other.dtime
    
    def __lt__(self, other):
        return self.dtime < other.dtime


def read_files(files):
    try:
        with open('resources{}session_files.db'.format(os.sep), 'br') as file_load:
            session_files = pickle.load(file_load)
    except:
        session_files = []    
    sessions = {}   # {'account_name': {'hostname': '', 'board': '', 'port': '', 'dtime': ''}}
    re_dslam =  re.compile(r'ST:\s+(.+?) atm 0/(\d+)/0/(\d+)')
    for file in files:
        if file.split('.')[-1] != 'csv':
            continue
        if file in session_files:
            continue
        print('Обработка файла {}'.format(file))
        with open(file,  encoding='windows-1251') as f:
            reader = csv.reader((line.replace('\0','') for line in f), delimiter=';')
            for row in reader:
                if not re_dslam.search(row[6]):
                    continue
                account_name = row[5]
                hostname = re_dslam.search(row[6]).group(1)
                board = re_dslam.search(row[6]).group(2)
                port = re_dslam.search(row[6]).group(3)
                dtime =  datetime.datetime.strptime(row[0], '%d.%m.%Y %H:%M:%S')
                session = Session(account_name, hostname, board, port, dtime)
                if account_name in sessions:
                    if session > sessions[account_name]:
                        sessions[account_name] = session
                else:
                    sessions[account_name] = session
        session_files.append(file)
    with open('resources{}session_files.db'.format(os.sep), 'bw') as file_dump:
            pickle.dump(session_files, file_dump)
    return sessions


def get_current_data():
    result = {}
    # Получение данных из базы данных
    options = {'table_name': 'abon_dsl',
               'str1': 'account_name, hostname, board, port',
               'str2': 'account_name IS NOT NULL'}
    accounts = SQL.get_table_data(**options)
    for account in accounts:
        result[account[0]] = {'hostname': account[1], 'board': account[2], 'port': account[3]}
    return result
    
    
def delete_files(files):
    for file in files:
        os.remove(file)


def main():
    # Просмотр файлов в директории input/update_table/
    files = ['input' + os.sep + 'update_table' + os.sep + x for x in os.listdir('input' + os.sep + 'update_table')]
    if len(files) == 0:
        return
    print("Начало работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    sessions = read_files(files)
    
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()    
    current_data = get_current_data()
    for session in sessions:
        if session not in current_data:
            continue
        if (sessions[session].hostname != current_data[session]['hostname']) or (sessions[session].board != current_data[session]['board']) or (sessions[session].port != current_data[session]['port']):
            options = {'cursor': cursor,
                       'table_name': 'abon_dsl',
                       'str1': 'hostname = "{}", board = {}, port = {}, valid = "yes"'.format(sessions[session].hostname, sessions[session].board, sessions[session].port),
                       'str2': 'account_name = "{}"'.format(session)}
            SQL.update_table(**options)
            #print('{}: {}/{}/{} --> {}/{}/{}'.format(session, current_data[session]['hostname'], current_data[session]['board'], current_data[session]['port'], sessions[session].hostname, sessions[session].board, sessions[session].port))
        else:
            options = {'cursor': cursor,
                       'table_name': 'abon_dsl',
                       'str1': 'valid = "yes"'.format(sessions[session].hostname, sessions[session].board, sessions[session].port),
                       'str2': 'account_name = "{}"'.format(session)}
            SQL.update_table(**options)            
    # Удаление файлов в директории
    #delete_files(files)
    print("Завершение работы: {}".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    print('---------\n')
    
    
if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)
    main()