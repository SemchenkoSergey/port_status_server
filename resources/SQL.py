# coding: utf-8

# SQL-запросы для других модулей

import MySQLdb
from resources import Settings


def create_data_dsl(drop=False):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    try:
        if drop:
            cursor.execute('DROP TABLE IF EXISTS data_dsl')
        table = '''
        CREATE TABLE IF NOT EXISTS data_dsl (
        id INT UNSIGNED NOT NULL AUTO_INCREMENT,
        hostname VARCHAR(50) NOT NULL,
        board TINYINT UNSIGNED NOT NULL,
        port TINYINT UNSIGNED NOT NULL,
        up_snr FLOAT(3,1),
        dw_snr FLOAT(3,1),
        up_att FLOAT(3,1),
        dw_att FLOAT(3,1),
        max_up_rate SMALLINT UNSIGNED,
        max_dw_rate SMALLINT UNSIGNED,
        up_rate SMALLINT UNSIGNED,
        dw_rate SMALLINT UNSIGNED,
        datetime DATETIME NOT NULL,
        CONSTRAINT pk_data_dsl PRIMARY KEY (id)
        )'''
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')
    try:
        command = '''
         CREATE INDEX idx_dslam ON data_dsl(hostname, board, port)
        '''
        cursor.execute(command)
        command = '''
         CREATE INDEX idx_datetime ON data_dsl(datetime)
        '''
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')    
    connect.close()    


def create_abon_dsl(drop=False):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    try:
        if drop:
            cursor.execute('DROP TABLE IF EXISTS abon_dsl')
        table = '''
        CREATE TABLE IF NOT EXISTS abon_dsl (
        phone_number CHAR(10) NOT NULL,
        area VARCHAR(30),
        locality VARCHAR(30),
        street VARCHAR(30),
        house_number VARCHAR(10),
        apartment_number VARCHAR(10),
        hostname VARCHAR(50),
        board TINYINT UNSIGNED,
        port TINYINT UNSIGNED,
        tariff SMALLINT UNSIGNED,
        account_name VARCHAR(20),
        tv ENUM('yes', 'no') DEFAULT 'no',
        timestamp TIMESTAMP,
        CONSTRAINT pk_abon_dsl PRIMARY KEY (phone_number)    
        )'''
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()
    
    
def create_abon_onyma(drop=False):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    try:
        if drop:
            cursor.execute('DROP TABLE IF EXISTS abon_onyma')
        table = '''
        CREATE TABLE IF NOT EXISTS abon_onyma (
        account_name VARCHAR(20) NOT NULL,
        bill VARCHAR(15) NOT NULL,
        dmid VARCHAR(15) NOT NULL,
        tmid VARCHAR(15) NOT NULL,
        CONSTRAINT pk_abon_onyma PRIMARY KEY (account_name)    
        )'''
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()


def create_data_sessions(drop=False):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    try:
        if drop:
            cursor.execute('DROP TABLE IF EXISTS data_sessions')
        table = '''
        CREATE TABLE IF NOT EXISTS data_sessions (
        account_name VARCHAR(20),
        date DATE,
        count SMALLINT UNSIGNED,
        CONSTRAINT pk_data_sessions PRIMARY KEY (account_name, date)
        )'''
        cursor.execute(table)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()
    
    
def delete_table(table_name, str1):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    try:
        command = '''
        DELETE
        FROM {}
        WHERE {}
        '''.format(table_name, str1)
        cursor.execute(command)
    except:
        pass
    else:
        cursor.execute('commit')
    connect.close()        



def get_all_table_data(table_name, str1):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    command = '''
    SELECT {}
    FROM {}
    '''.format(str1, table_name)
    cursor.execute(command)
    result = cursor.fetchall()
    connect.close()
    return result


def get_table_data(table_name, str1, str2):
    connect = MySQLdb.connect(host=Settings.db_host, user=Settings.db_user, password=Settings.db_password, db=Settings.db_name, charset='utf8')
    cursor = connect.cursor()
    command = '''
    SELECT {}
    FROM {}
    WHERE {}
    '''.format(str1, table_name, str2)
    cursor.execute(command)
    result = cursor.fetchall()
    connect.close()
    return result
        

def insert_table(cursor, table_name, str1, str2):
    command = '''
    INSERT INTO {}
    ({})
    VALUES
    ({})
    '''.format(table_name, str1, str2)
    try:
        cursor.execute(command)
    except Exception as ex:
        print(ex)
    else:
        cursor.execute('commit')
        

def update_table(cursor, table_name, str1, str2):
    command ='''
    UPDATE {}
    SET {}
    WHERE {}
    '''.format(table_name, str1, str2)
    try:
        cursor.execute(command)
    except Exception as ex:
        print(ex)
    else:
        cursor.execute('commit')