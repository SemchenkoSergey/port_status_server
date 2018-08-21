# coding: utf-8

import re
import sys
import requests
from resources import Settings
from bs4 import BeautifulSoup as BS
#import Settings
import warnings
warnings.filterwarnings("ignore")

def get_onyma():
    session = requests.Session()
    auth_url = 'https://10.144.196.37/onyma/login.htms'
    auth_payload = {'LOGIN': Settings.onyma_login, 'PASSWD': Settings.onyma_password, 'enter': 'Вход'}
    try:
        result = session.post(auth_url, data=auth_payload, verify=False)
    except:
        print('https://10.144.196.37/onyma/login.htms не доступен. Проверьте соединение с сетью.')
        return None
    if 'AUTH_ERR' in result.text:
        print('Не верные логин/пароль!')
        return None
    return session


def count_sessions(onyma, bill,  dmid,  tmid,  date):
    result = {}
    re_hostname = r'(STV[\w-]+?) atm \d/(\d+)/\d/(\d+)'
    try:
        html = onyma.get("https://10.144.196.37/onyma/main/ddstat.htms?bill={}&dt={}&mon={}&year={}&service=201&dmid={}&tmid={}".format(bill,  date.day,  date.month, date.year,  dmid,  tmid))        
        result['count'] = int(re.search(r'<td class="foot">Все</td><td class="pgout" colspan="5">.+?<b>(\d+)</b>',  html.text.__repr__()).group(1))
        argus_data = re.search(re_hostname, html.text.__repr__())
        if argus_data:
            result['hostname'] = argus_data.group(1)
            result['board'] = int(argus_data.group(2))
            result['port'] = int(argus_data.group(3))
        else:
            result['hostname'] = None
            result['board'] = None
            result['port'] = None             
    except:
        return -1
    return result


def update_tv(onyma, bill, date):
    try:
        html = onyma.get("https://10.144.196.37/onyma/main/mstat.htms?bill={}&mon={}&year={}".format(bill, date.month, date.year))
    except:
        return -1
    if 'Предоставление услуг IPTV' in html.text:
        return True
    else:
        return False


def find_account_param(onyma, account_name):
    url_ip = 'https://10.144.196.37'
    url_main = 'https://10.144.196.37/onyma/main/'
    try:
        html = onyma.post('https://10.144.196.37/onyma/main/dogsearch_ok.htms', {'sitename': account_name, 'search': 'Поиск'}, verify=False).text
        url = BS(html, 'lxml').find('a', title=re.compile('Договор')).get('href')
        html = onyma.get(url_ip + url).text
        url = BS(html, 'lxml').find('a', id='menu4185').get('href')
        html = onyma.get(url_ip + url).text
        url = url_main + BS(html, 'lxml').find('td', class_='td1').find('a').get('href')
        html = onyma.get(url).text
        tds = BS(html, 'lxml').find_all('td', class_='td1')
    except:
        return -1
    urls = []
    for td in tds:
        try:
            url = td.find('a').get('href')
        except:
            continue
        if ('service=201' in url) and (td.find('a').text == account_name):
            urls.append(url_main + url)
    result_url = ''
    result_date = 1
    for url in urls:
        try:
            html = onyma.get(url).text
            current_date = int(BS(html, 'lxml').find('td', class_='td1').find('a').text.split('.')[0])
        except:
            continue            
        if current_date >= result_date:
            result_date = current_date
            result_url = url
    if result_url != '':
        bill = re.search(r'bill=(\d+)', result_url).group(1)
        dmid = re.search(r'dmid=(\d+)', result_url).group(1)
        tmid = re.search(r'tmid=(\d+)', result_url).group(1)
        return bill, dmid, tmid
    else:
        return False


def get_speed(tariff):
    re_speed_kb = re.compile(r'(\d+) ?[k|К]')
    re_speed_mb = re.compile(r'(\d+) ?Мбит')
    
    if re_speed_mb.search(tariff):
        speed = int(re_speed_mb.search(tariff).group(1)) * 1024
    elif re_speed_kb.search(tariff):
        speed = int(re_speed_kb.search(tariff).group(1))
    elif 'Максимальная скорость'.lower() in tariff.lower():
        return 15 * 1024
    elif 'Нон-стоп max'.lower() in tariff.lower():
        return 15 * 1024
    elif 'тариф "Игровой"'.lower() in tariff.lower():
        return 15 * 1024    
    else:
        return False
    return speed
    
    
def find_account_speed(onyma, account_name):
    url_ip = 'https://10.144.196.37'
    url_main = 'https://10.144.196.37/onyma/main/'
    try:
        html = onyma.post('https://10.144.196.37/onyma/main/dogsearch_ok.htms', {'sitename': account_name, 'search': 'Поиск'}, verify=False).text
        url = BS(html, 'lxml').find('a', title='Персональный тариф').get('href')
        html = onyma.get(url_ip + url).text
        tds = BS(html, 'lxml').find_all('td', class_='td1')
    except:
        return -1
    rows = int(len(tds)/6)
    speed = 0
    tariffs = []
    for row in range(0, rows):
        tariffs.append((tds[row * 6].text.strip(), tds[row * 6 + 1].text.strip(), tds[row*6 + 4].text.strip()))
    for tariff in tariffs:
        if tariff[2] == '':
            for i in (0,1):
                cur_speed = get_speed(tariff[i])
                if cur_speed is not False:
                    if cur_speed > speed:
                        speed = cur_speed
    if speed > 0:
        return speed
    else:
        return False


def find_argus_id(onyma, onyma_id):
    try:
        html = onyma.post('https://10.144.196.37/onyma/main/dogsearch_ok.htms', {'clsrv': onyma_id, 'search': 'Поиск'}, verify=False).text
        url = BS(html, 'lxml').find('a', title='Учет оборудования').get('href')
        result = re.search(r'&rec=(.+?)&', url).group(1)
    except:
        return -1
    return result