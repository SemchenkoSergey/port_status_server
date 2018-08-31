#!/usr/bin/env python3
# coding: utf8

import datetime
import os
import pickle
import subprocess
from resources import Settings
from resources import DslamHuawei
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
    response = subprocess.call('ping -c 1 {}'.format(ip), shell='True', stdout=DEVNULL, stderr=subprocess.STDOUT)
    if response != 0:
        return None
    if model == '5600':
        try:
            dslam = DslamHuawei.DslamHuawei5600(ip, Settings.login_5600, Settings.password_5600, 20)
        except:
            return None
    elif model == '5616':
        try:
            dslam = DslamHuawei.DslamHuawei5616(ip, Settings.login_5616, Settings.password_5616, 20)
        except:
            return None
    else:
        return None
    return dslam

def run(host):
    dslam = connect_dslam(host)
    if dslam is None:
        print('Не удалось подключиться к DSLAM {}'.format(host[0]))
        return (0, host[0])
    result = dslam.add_user('script_profile', 'script_user', 'script1password')
    if result:
        print('Обработан DSLAM {}'.format(dslam.hostname))
        del dslam
        return (1, host[0])
    else:
        print('DSLAM {} не обработан'.format(dslam.hostname))
        del dslam
        return (0, host[0])


def main():
    # Загрузка списка DSLAM
    dslams = load_dslams()
    if len(dslams) == 0:
        print('Не найден dslams.db!')
        return    
    dslam_ok = 0
    dslam_bad = []
    
    # Запуск основного кода
    current_time = datetime.datetime.now()
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(run, dslams)
    
    for result in results:
        if result is None:
            continue
        elif result[0] == 1:
            dslam_ok += 1
        else:
            dslam_bad.append(result[1])
     
    print('Время: {}'.format(current_time.strftime('%Y-%m-%d %H:%M')))
    print('Всего DSLAM: {}'.format(len(dslams)))
    print('Обработано: {}'.format(dslam_ok))
    print('Необработанные: {}'.format(', '.join(dslam_bad)))
    print('---------\n')



if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)
    main()