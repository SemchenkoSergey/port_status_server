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

#DslamHuawei.LOGGING = True

def get_dslams_ip(files):
    dslams_ip = []
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
                if (model == 'Huawei MA 5616') or (model == 'Huawei MA 5600'):
                    dslams_ip.append(ip)
    return dslams_ip


def load_dslams():
    try:
        with open('resources{}dslams.db'.format(os.sep), 'br') as file_load:
            return pickle.load(file_load)
    except:
        return []


def test_dslam(ip, out):
    response = subprocess.call('ping -c 3 {}'.format(ip), shell='True', stdout=out, stderr=subprocess.STDOUT)
    if response == 0:
        return True
    else:
        return False

def connect_dslam(ip):
    #Создание объекта dslam
    try:
        dslam = DslamHuawei.DslamHuawei(ip, Settings.login, Settings.password, 20)
    except:
        return None
    else:
        return dslam
    

def check_device_type(arguments):
    ip = arguments[0]
    out = arguments[1]
    if not test_dslam(ip, out):
        return None
    dslam = connect_dslam(ip)
    if dslam is None:
        return None
    device_type = dslam.get_device_type()
    del dslam
    if device_type == '5600':
        return (ip, '5600')
    elif device_type == '5616':
        return (ip, '5616')
    else:
        return None
    

def delete_files(files):
    for file in files:
        os.remove(file)


def main():
    # Просмотр файлов в директории input/dslams/
    files = ['input' + os.sep + 'dslams' + os.sep + x for x in os.listdir('input' + os.sep + 'dslams')]
    if len(files) == 0:
        return    
    # Обработка файлов
    print("Запуск программы: {}\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))
    dslams_ip = get_dslams_ip(files)
    print('Обнаружено DSLAM: {}'.format(len(dslams_ip)))
    delete_files(files)
    
    # Проверка доступности и типов DSLAM
    print('Проверка типов DSLAM...')
    DEVNULL = os.open(os.devnull, os.O_WRONLY)
    arguments = [(ip, DEVNULL) for ip in dslams_ip]
    with ThreadPoolExecutor(max_workers=Settings.threads) as executor:
        results = executor.map(check_device_type, arguments)    
    
    dslams = []
    for result in results:
        if result is None:
            continue
        dslams.append((result[0], result[1]))
        #print(result[0], result[1])
    
    print('Доступно DSLAM: {}'.format(len(dslams)))
    
    with open('resources{}dslams.db'.format(os.sep), 'bw') as file_dump:
            pickle.dump(dslams, file_dump)    
    

if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()
