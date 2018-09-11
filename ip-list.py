#!/usr/bin/env python3
# coding: utf8

import os
import csv
import pickle
import datetime
import subprocess
from resources import Settings
from resources import DslamHuawei
from concurrent.futures import ThreadPoolExecutor
import warnings

warnings.filterwarnings("ignore")

DslamHuawei.LOGGING = True

def generate_dslams(files):
    dslams = []
    for file in files:
        if file.split('.')[-1] != 'csv':
            continue
        with open(file,  encoding='windows-1251') as f:
            print('Обработка {}'.format(file))
            reader = csv.reader(f, delimiter=';')
            for row in reader:
                if len(row) < 35:
                    continue
                elif 'Российская Федерация Гос-во' not in row[2]:
                    continue
                elif 'sklad' in row[5].lower() or 'test' in row[5].lower():
                    continue
                model = row[4].replace('=', '').replace('"', '')
                ip = row[6].replace('=', '').replace('"', '')
                if ip == '':
                    continue
                if model == 'Huawei MA 5616':
                    dslams.append((ip, '5616'))
                elif model == 'Huawei MA 5600':
                    dslams.append((ip, '5600'))
    with open('resources{}dslams.db'.format(os.sep), 'bw') as file_dump:
            pickle.dump(dslams, file_dump)
    return len(dslams)


def load_dslams():
    try:
        with open('resources{}dslams.db'.format(os.sep), 'br') as file_load:
            return pickle.load(file_load)
    except:
        return []


def test_dslam(dslam, out):
    response = subprocess.call('ping -c 3 {}'.format(dslam[0]), shell='True', stdout=out, stderr=subprocess.STDOUT)
    if response == 0:
        return True
    else:
        return False

def connect_dslam(host, default=False):
    #Создание объекта dslam
    ip = host[0]
    model = host[1]
    if model == '5600':
        try:
            if default:
                dslam = DslamHuawei.DslamHuawei5600(ip, Settings.login_5600, Settings.password_5600, 20)
            else:
                dslam = DslamHuawei.DslamHuawei5600(ip, Settings.login, Settings.password, 20)
        except:
            return None
    elif model == '5616':
        try:
            if default:
                dslam = DslamHuawei.DslamHuawei5616(ip, Settings.login_5616, Settings.password_5616, 20)
            else:
                dslam = DslamHuawei.DslamHuawei5616(ip, Settings.login, Settings.password, 20)
        except:
            return None
    else:
        return None
    return dslam

def test_username(arguments):
    # Проверка логина/пароля на DSLAM
    host = arguments[0]
    out = arguments[1]
    if not test_dslam(host, out):
        return (host[0], '', '')
    dslam = connect_dslam(host)
    if dslam is None:
        dslam = connect_dslam(host, default=True)
        if dslam is None:
            return ('', host[0], '')
        else:
            result = dslam.add_user('script_profile', '', '')
            if result:
                del dslam
                return ('', '', host[0])
            else:
                print('Не удалось создать юзера на {}'.format(host[0]))
                del dslam


def delete_files(files):
    for file in files:
        os.remove(file)


def main():
    # Просмотр файлов в директории input/ip_list/
    files = ['input' + os.sep + 'ip_list' + os.sep + x for x in os.listdir('input' + os.sep + 'ip_list')]
    if len(files) == 0:
        return    
    # Обработка файлов
    print("Запуск программы: {}\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))
    count = generate_dslams(files)
    print('Обнаружено DSLAM: {}'.format(count))
    #dslams = load_dslams()
    #for idx, dslam in enumerate(dslams):
        #print('{}: {}'.format(idx, dslam))
    #print()
    delete_files(files)
    
    # Проверка доступности DSLAM и учетного имени
    print('Проверка учетных имен на DSLAM...')
    DEVNULL = os.open(os.devnull, os.O_WRONLY)
    dslam_unavailable = []
    dslam_wrong_log_pass = []
    dslam_change = []
    dslams = load_dslams()
    arguments = [(host, DEVNULL) for host in dslams]
    with ThreadPoolExecutor(max_workers=Settings.threads) as executor:
        results = executor.map(test_username, arguments)    
    
    for result in results:
        if result is None:
            continue
        if result[0] != '':
            dslam_unavailable.append(result[0])
        elif result[1] != '':
            dslam_wrong_log_pass.append(result[1])
        elif result[2] != '':
            dslam_change.append(result[2])
    
    print('Добавлен User - {}: {}'.format(len(dslam_change), ', '.join(dslam_change)))
    print('Не проходит стандартный log/pass - {}: {}'.format(len(dslam_wrong_log_pass), ', '.join(dslam_wrong_log_pass)))
    print('Не доступные DSLAM - {}: {}'.format(len(dslam_unavailable), ', '.join(dslam_unavailable)))

if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()