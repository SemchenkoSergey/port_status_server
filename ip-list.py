#!/usr/bin/env python3
# coding: utf8

import csv
import os
import pickle



def generate_dslams():
    dslams = []
    with open('список.csv',  encoding='windows-1251') as f:
        reader = csv.reader(f, delimiter=';')
        for row in reader:
            if len(row) < 35:
                continue
            elif 'Российская Федерация Гос-во' not in row[2]:
                continue
            model = row[4].replace('=', '').replace('"', '')
            ip = row[6].replace('=', '').replace('"', '')
            if model == 'Huawei MA 5616':
                dslams.append((ip, '5616'))
            elif model == 'Huawei MA 5600':
                dslams.append((ip, '5600'))
    with open('dslams', 'bw') as file_dump:
            pickle.dump(dslams, file_dump)

def load_dslams():
    try:
        with open('dslams', 'br') as file_load:
            return pickle.load(file_load)
    except:
        return []

def get_ip_list(dslams):
    result = ''
    number = 1
    for dslam in dslams:
        if len(result + dslam[0]) < 239:
            result += dslam[0] + ';'
        else:
            print('{} строка для формирования отчета: {}\n'.format(number, result))
            result = dslam[0] + ';'
            number += 1
    print('{} строка для формирования отчета: {}\n'.format(number, result))
    




def main():
    generate_dslams()
    dslams = load_dslams()
    for dslam in dslams:
        print(dslam)
    print()
    get_ip_list(dslams)


if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()