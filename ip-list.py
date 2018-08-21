#!/usr/bin/env python3
# coding: utf8

import os
from resources import Settings

def main():
    out = ''
    for host in Settings.hosts:
        if len(out + host[0]) < 239:
            out += host[0] + ';'
        else:
            print('Строка для формирования отчета: {}\n'.format(out))
            out = host[0] + ';'
    print('Строка для формирования отчета: {}'.format(out))

if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()
