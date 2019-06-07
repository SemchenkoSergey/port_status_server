Используемый в программе модуль pexpect работает в ОС Linux. Работа возможно только с DSLAM Huawei MA5600 и Huawei MA5616.


Установка программ и модулей python:
------------------------------------
sudo apt install python3
sudo apt install python3-pip
sudo apt install mariadb-server
sudo apt install default-libmysqlclient-dev 
sudo pip3 install mysqlclient
sudo pip3 install pexpect



Настройка базы данных:
----------------------
sudo mysql -p

Создание пользователя 'inet' с паролем 'inet' (для работы программы)
CREATE USER 'inet'@'localhost' IDENTIFIED BY 'inet';

Наделение пользователя 'inet' всеми полномочиями
GRANT ALL PRIVILEGES ON *.* TO 'inet'@'localhost';

Создание пользователя 'operator' с паролем 'operator' (для выполнения запросов)
CREATE USER 'operator'@'%' IDENTIFIED BY 'operator';

Наделение пользователя 'operator' возможностью выполнять запрос SELECT
GRANT SELECT ON *.* TO 'operator'@'%';

Обновление прав доступа
FLUSH PRIVILEGES;

Создание таблицы для работы программы
CREATE DATABASE adsl CHARACTER SET utf8;

SET GLOBAL sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

Выход
exit

Подключение к базе данных 'inet' под учеткой 'operator'
mysql -u operator -poperator inet


Настройки программ:
-------------------

Файл настроек 'resources\Settings.py'

Нужно заполнить логины/пароли (кавычки не убирать)
login_5600 = ''
password_5600 = ''
login_5616 = ''
password_5616 = ''

Количество одновременно обрабатываемых DSLAM
threads = 20

За сколько дней хранить данные в базе
days = 14

Настройки для доступа к базе данных
db_host = 'localhost'
db_user = ''
db_password = ''
db_name = 'adsl'


Запуск программ:
----------------
Программы запускаются вручную либо через crontab.

ip-list.py - читает файлы в директории 'input\ip_list' и формирует список ip адресов DSLAM
make_table.py - читает файлы в директории 'input\make_table\[argus, onyma]' и формирует таблицу abon_dsl в базе данных
update_table.py - читает файлы в директории 'input\update_table' и обновляет данные по hostnamt, board, port в таблице abon_dsl на актуальные
port_status.py - считывает статистику с DSLAM и заносит ее в базу данных data_dsl


Отчеты из Аргуса и Онимы:
-------------------------
Отчеты должны быть выгружены в формате csv, могут иметь любое имя (но с расширением .csv)
Onyma:
 'Абонентские сессии'	в директорию 'input\update_table'
 'Список подключений' 	в директорию 'input\make_table\onyma'
Argus:
 'Услуги по региону' 	в директорию 'input\make_table\argus'
 'Список DSLAM' 		в директорию 'input\ip_list'


Использование базы данных:
--------------------------
Для подключения к базе данных в терминале выполнить команду
mysql -u operator -poperator inet

Скопировать любой SQL-запрос из файла и вставить в окно терминала. Дождаться вывода.


Для работы некоторых запросов:
1. в терминале
sudo nano /etc/mysql/my.cnf
2. в конце файла дописываем строки
[mysqld]
sql_mode="NO_ENGINE_SUBSTITUTION"
innodb_buffer_pool_size=2G
3. сохраняем файл и restart mysql
sudo systemctl restart mysql

Выгрузка запроса в CSV-файл:
SELECT * FROM name_table
INTO OUTFILE '/tmp/name_file.csv' FIELDS TERMINATED BY ';' ;
