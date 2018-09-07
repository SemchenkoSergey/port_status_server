#!/usr/bin/env python3
# coding: utf8

import os
import csv
import pickle
import datetime
import subprocess


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
    response = subprocess.call('ping -c 1 {}'.format(dslam[0]), shell='True', stdout=out, stderr=subprocess.STDOUT)
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
    for dslam in dslams:
        if not test_dslam(dslam, DEVNULL):
            dslam_unavailable.append(dslam[0])
            continue
        obj_dslam = connect_dslam(dslam)
        if obj_dslam is None:
            obj_dslam = connect_dslam(dslam, default=True)
            if obj_dslam is None:
                dslam_wrong_log_pass.append(dslam[0])
                continue
            else:
                result = dslam.add_user('script_profile', 'script_user', 'script1password')
                if result:
                    dslam_change.append(dslam[0])
                    del dslam
                else:
                    print('Не удалось создать юзера на {}'.format(dslam[0]))
                    del dslam
    
    print('Добавлен User: {}'.format(', '.join(dslam_change)))
    print('Не проходит стандартный log/pass: {}'.format(', '.join(dslam_wrong_log_pass)))
    print('Не доступные DSLAM: {}'.format(', '.join(dslam_unavailable)))

if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()