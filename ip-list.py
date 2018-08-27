#!/usr/bin/env python3
# coding: utf8

import os
import csv
import pickle
import datetime


def generate_dslams(files):
    dslams = []
    for file in files:
        if file.split('.')[-1] != 'csv':
            continue        
        with open(file,  encoding='windows-1251') as f:
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


def load_dslams():
    try:
        with open('resources{}dslams.db'.format(os.sep), 'br') as file_load:
            return pickle.load(file_load)
    except:
        return []


def print_ip_list(dslams):
    result = ''
    number = 1
    for dslam in dslams:
        if len(result + dslam[0]) < 239:
            result += dslam[0] + ';'
        else:
            print('{}: {}'.format(number, result))
            result = dslam[0] + ';'
            number += 1
    print('{}: {}\n'.format(number, result))


def delete_files(files):
    for file in files:
        os.remove(file)


def main():
    # Просмотр файлов в директории input/ip_list/
    files = ['input' + os.sep + 'ip_list' + os.sep + x for x in os.listdir('input' + os.sep + 'ip_list')]
    if len(files) == 0:
        return    
    # Обработка файлов
    print("Обработка файлов: {}\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))
    generate_dslams(files)
    dslams = load_dslams()
    #for idx, dslam in enumerate(dslams):
        #print('{}: {}'.format(idx, dslam))
    #print()
    print('Наборы ip для формирования отчетов:')
    print_ip_list(dslams)
    print()
    delete_files(files)


if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()