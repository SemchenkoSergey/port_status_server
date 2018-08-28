#!/usr/bin/env python3
# coding: utf8

import os
import pickle
import subprocess

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
    

def main():
    dslams = load_dslams()
    if len(dslams) == 0:
        return
    dslams_ok = []
    dslams_bad = []
    DEVNULL = os.open(os.devnull, os.O_WRONLY)
    #for idx, dslam in enumerate(dslams):
        #print('{}: {}'.format(idx, dslam))
    #print()
    
    for dslam in dslams:
        test = test_dslam(dslam, DEVNULL)
        if test:
            #print('{} отвечает'.format(dslam[0]))
            dslams_ok.append(dslam[0])
        else:
            #print('{} не отвечает'.format(dslam[0]))
            dslams_bad.append(dslam[0])
    
    print('Всего DSLAM: {}'.format(len(dslams)))
    print('Отвечают: {}'.format(len(dslams_ok)))
    print('Не отвечают: {}'.format(len(dslams_bad)))
    print('Не отвечают: {}\n'.format(', '.join(dslams_bad)))
    




if __name__ == '__main__':
    cur_dir = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-1])
    os.chdir(cur_dir)    
    main()