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
        cur_dslams = []
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
                    cur_dslams.append((ip, '5616'))
                elif model == 'Huawei MA 5600':
                    cur_dslams.append((ip, '5600'))
        print_ip_list(file, cur_dslams)
        dslams.extend(cur_dslams)
    with open('resources{}dslams.db'.format(os.sep), 'bw') as file_dump:
            pickle.dump(dslams, file_dump)


def load_dslams():
    try:
        with open('resources{}dslams.db'.format(os.sep), 'br') as file_load:
            return pickle.load(file_load)
    except:
        return []


def print_ip_list(file, dslams):
    if len(dslams) == 0:
        return
    print('\r\nНаборы ip для формирования отчетов из файла {}:'.format(file))
    result = ''
    number = 1
    for dslam in dslams:
        if len(result + dslam[0]) < 239:
            result += dslam[0] + ';'
        else:
            print('{}: {}'.format(number, result))
            result = dslam[0] + ';'
            number += 1
    print('{}: {}\r\n'.format(number, result))


def delete_files(files):
    for file in files:
        os.remove(file)


def main():
    # Просмотр файлов в директории input/ip_list/
    files = ['input' + os.sep + 'ip_list' + os.sep + x for x in os.listdir('input' + os.sep + 'ip_list')]
    if len(files) == 0:
        return    
    # Обработка файлов
    print("Обработка файлов: {}\r\n".format(datetime.datetime.now().strftime('%Y-%m-%d %H:%M')))
    generate_dslams(files)
    dslams = load_dslams()
    #for idx, dslam in enumerate(dslams):
        #print('{}: {}'.format(idx, dslam))
    #print()
    #delete_files(files)


if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()