# coding: utf-8

### Port_Status ###
# Учетные данные DSLAM
login_5600 = ''
password_5600 = ''
login_5616 = ''
password_5616 = ''

#Количество потоков выполнения
threads = 5

#За сколько дней хранить записи
days = 35

#Список DSLAM
hosts = (('ip', '5600'),
         ('ip', '5616'))
         
# Mysql
db_host = 'localhost'
db_user = 'inet'
db_password = 'inet'
db_name = 'inet'

### Session_Count ###
threads_count = 3
# Onyma
onyma_login = ''
onyma_password = ''
