# coding: utf-8

import pexpect
import datetime
import time
import re
import os

LOGGING = False
LINE_PROFILE = False

class DslamHuawei():
    def __init__(self, ip, login, password,  timeout):
        self.ip = ip
        self.connect(login,  password,  timeout)

        # Распознавание версии ПО
        str_out = self.write_read_data('display version')
        version = re.search(r'\b(MA.+?)\b', str_out)
        if version:
            self.version = version.group(1)
        else:
            self.version = '-'
        self.adsl_line_profile = {}
        if LINE_PROFILE:
            self.set_adsl_line_profile()
            
    
    def __del__(self):
        self.tn.close()
    
    def connect(self, login, password,  timeout):
        """ Подключиться к DSLAM """
        self.tn = pexpect.spawn('telnet {}'.format(self.ip))
        if LOGGING:
            if not os.path.exists('dslam_logs'):
                os.mkdir('dslam_logs')   
            self.tn.logfile = open('dslam_logs{}{} {}.txt'.format(os.sep, self.ip,  datetime.datetime.now().strftime('%Y-%m-%d')), 'ab')
        self.tn.expect('>>User name:')
        self.tn.sendline(login)
        self.tn.expect('>>User password:')
        self.tn.sendline(password)
        num = self.tn.expect([ '>>User name:', '(>|\) ----)'])
        if num == 0:
            raise Exception('{}: Invalid login/password!'.format(self.ip))
        self.tn.sendline(' ')
        self.tn.expect('>')
        self.tn.sendline('enable')
        self.tn.expect('#')
        # Распознавание hostname
        self.hostname = re.search('\n([\w-]+)$', self.tn.before.decode('utf-8')).group(1)        
        commands = ['undo smart',
                    'undo interactive',
                    'idle-timeout {}'.format(timeout),
                    'scroll 512',
                    'undo alarm output all']
        for command in commands:
            self.write_read_data(command,  short=True)
    
    def alive(self):
        """ Проверка есть ли связь с DSLAM """
        str_out = self.write_read_data('',  short=True)
        if str_out == '\n\n{}'.format(self.hostname):
            return True
        elif str_out is False:
            return False
    
    def write_data(self, command):
        """ Отправка команды """
        command_line = command
        self.tn.sendline(command_line)
        return True
    
    def clean_out(self):
        while True:
            try:
                self.tn.expect('#', timeout=10)
            except:
                break
            
    def check_out(self, command, str_out, short):
        """ Проверка вывода команды """
        bad_strings = ('Failure: System is busy', 'Failure: The command is being executed', 'please wait',  'Unknown command', 'percentage of saved data')
        if not re.search(r'^\n*{}'.format(command), str_out):
            #print('1 - {}({}) {}:\n{}'.format(self.hostname, self.ip, command, str_out))
            return False
        if (not short) and (str_out.replace('\n', '').replace(self.hostname, '').strip() == command):
            #print('2 - {}({}) {}:\n{}'.format(self.hostname, self.ip, command, str_out))
            return False
        for string in bad_strings:
            if string in str_out:
                #print('3 - {}({}) {}:\n{}'.format(self.hostname, self.ip, command, str_out))
                return False
        return True    

    def read_data(self, command,  short):
        """ Чтение данных """
        command_line = command
        result = ''
        while True:
            try:
                self.tn.expect('#', timeout=120)
            except Exception as ex:
                print('{}({}): ошибка чтения. Команда - {}'.format(self.hostname, self.ip, command_line))
                print(str(ex).split('\n')[0])
                return False
            else:
                result += re.sub(r'[^A-Za-z0-9\n\./: _-]|(.\x1b\[..)', '', self.tn.before.decode('utf-8'))
                if result.count('\n') == 2 and not short:
                    continue
                if self.check_out(command_line, result, short):              
                    return result
                else:
                    #print('{}({}) - {}\nBad output:\n{}'.format(self.hostname, self.ip, command, result))
                    return False              
        
    def write_read_data(self, command,  short=False):
        """ Выполнение команды и получение результата """
        command_line = command
        for count in range(0, 3):
            self.write_data(command_line)
            result = self.read_data(command_line,  short)
            if result is not False:
                return result
            else:
                time.sleep(45)
                self.write_data(' ')
                self.clean_out()
        print('{}({}): не удалось обработать команду {}'.format(self.hostname, self.ip, command_line))
        return False

    def set_boards(self, boards_list):
        """ Установить self.boards - список плат """
        regex = r'Main Board:\s+H\d+AD'
        self.boards = []
        for board in boards_list:
            command_line = 'display version 0/{}'.format(board)
            str_out = self.write_read_data(command_line)
            if str_out is False:
                return False
            if ('Failure' not in str_out) and ('% Parameter error' not in str_out) and (re.search(regex, str_out)):
                self.boards.append(board) 
    
    def set_adsl_line_profile(self):
        """ Установить self.adsl_line_profile - список профайлов линий """
        command_line = 'display adsl line-profile'
        str_out = self.write_read_data(command_line)
        if str_out is False:
            return False
        #self.adsl_line_profile = {}
        prev_name = ''
        prev_index = ''
        prev_up_rate = ''
        prev_dw_rate = ''
        for line in str_out.split('\n'):
            current_index = line[0:10].strip()
            current_name = line[10:21].strip()
            current_dw_rate = line[54:65].strip()
            current_up_rate = line[74:80].strip()
            try:
                int(current_index)
            except:
                if prev_index == '':
                    continue
                if current_name == '-----------':
                    self.adsl_line_profile[int(prev_index)] = {'profile_name' : prev_name, 'dw_rate': prev_dw_rate, 'up_rate' : prev_up_rate}
                    break
                prev_name += current_name
                continue
            if prev_index != '' and prev_name != '':
                self.adsl_line_profile[int(prev_index)] = {'profile_name' : prev_name, 'dw_rate': prev_dw_rate, 'up_rate' : prev_up_rate}
            prev_index = current_index
            prev_name = current_name
            prev_dw_rate = current_dw_rate
            prev_up_rate = current_up_rate
    
    def get_info(self):
        """ Получить информацию о DSLAM """
        return {'ip' : self.ip,
                'hostname' : self.hostname,
                'version' : self.version,
                'model' : self.model}
    
    def get_activated_ports(self, board):
        """ Получить список активированных портов с платы """
        if board not in self.boards:
            return []
        regex = re.compile(r' +(\w*) +ADSL +Activated')
        command_line = 'display board 0/{}'.format(board)
        str_out = self.write_read_data(command_line)
        if str_out is False:
            return False
        result = []
        for line in str_out.split('\n'):
            match = regex.search(line)
            if match:
                result.append(int(match.group(1)))
        return result       
    
    def get_line_operation_board(self, board):
        """ Получить список параметров линий с активированных портов """
        command = 'display line operation board'
        template = {'up_snr' : '-',
                    'dw_snr' : '-',
                    'up_att' : '-',
                    'dw_att' : '-',
                    'max_up_rate' : '-',
                    'max_dw_rate' : '-',
                    'up_rate' : '-',
                    'dw_rate' : '-'}
        result = [template for x in range(0, self.ports)]
        command_line = '{} 0/{}'.format(command, board)
        str_out = self.write_read_data(command_line)
        if str_out is False:
            return False
        if 'Xdsl UpNoise DwNoise Up Stream Dw Stream MaxUp MaxDw UpOut DwOut Up    Dw' in str_out:
            regex = r"( *-?\d+\.?\d*){11}"
        elif 'XP UNM  DNM   USA   DSA  MUR   MDR   UOP   DOP   UR    DR    CES     RES    IT' in str_out:
            regex = r"( *-?\d+\.?\d*){14}"
        elif ('Failure: Port is not active' in str_out) or ('Failure: No port has been activated' in str_out):
            return result
        else:
            print('{}, {}: {} - не удалось распознать вывод {}'.format(self.ip, self.hostname, board, command_line))
            return result
        matches = re.finditer(regex, str_out)
        if matches:
            for match in matches:
                match_list = list(match.group(0).split())
                if len(match_list) < 11:
                    continue
                result[int(match_list[0])] = {'up_snr' : float(match_list[1]),
                                              'dw_snr' : float(match_list[2]),
                                              'up_att' : float(match_list[3]),
                                              'dw_att' : float(match_list[4]),
                                              'max_up_rate' : float(match_list[5]),
                                              'max_dw_rate' : float(match_list[6]),
                                              'up_rate' : float(match_list[9]),
                                              'dw_rate' : float(match_list[10])}
        return result        
    
    def get_line_operation_port(self, board, port):
        """ Получить параметры линии с порта """
        command = 'display line operation port'
        command_line = '{} 0/{}/{}'.format(command, board, port)
        str_out = self.write_read_data(command_line)
        if str_out is False:
            return False
        if re.search(r'Failure:.+is not activ(e|ated)', str_out):
            return {}
        dw_rate = float(re.search(r'Downstream (channel|actual).+rate.+: (\d+)', str_out).group(2))
        max_dw_rate = float(re.search(r'Downstream max.+: (\d+)', str_out).group(1))
        dw_snr = float(re.search(r'Downstream channel SNR.+: (.+)', str_out).group(1))
        dw_att = float(re.search(r'Downstream channel attenuation.+: (.+)', str_out).group(1))
        up_rate = float(re.search(r'Upstream (channel|actual).+rate.+: (\d+)', str_out).group(2))
        max_up_rate = float(re.search(r'Upstream max.+: (\d+)', str_out).group(1))
        up_snr = float(re.search(r'Upstream channel SNR.+: (.+)', str_out).group(1))
        up_att = float(re.search(r'Upstream channel attenuation.+: (.+)', str_out).group(1))
        return {'dw_rate' : dw_rate,
                'max_dw_rate' : max_dw_rate,
                'dw_snr' : dw_snr,
                'dw_att' : dw_att,
                'up_rate' : up_rate,
                'max_up_rate' : max_up_rate,
                'up_snr' : up_snr,
                'up_att' : up_att}
    
    def get_mac_address_port(self, board, port):
        """ Получить список MAC-адресов с порта """
        result = []
        regex = r"\b([0-9a-f-]{14})\b.*\b(\d+)"
        command = 'display mac-address port'
        command_line = '{} 0/{}/{}'.format(command, board,  port)
        str_out = self.write_read_data(command_line)
        if str_out is False:
            return False
        if 'Failure:' in str_out:
            return []
        elif 'Total:' in str_out:
            matches = re.finditer(regex, str_out)
            if matches:
                for match in matches:
                    result.append((match.group(1), match.group(2)))
            return result
    
    def get_adsl_line_profile(self, profile_index):
        """ Получить описание профайла линии по его индексу """
        if profile_index not in self.adsl_line_profile:
            return 'The profile does not exist'
        command = 'display adsl line-profile'
        command_line = '{} {}'.format(command, profile_index)
        str_out = self.write_read_data(command_line)
        if str_out is False:
            return False
        result = ''
        for line in str_out.split('\n'):
            if len(line) < 1:
                continue
            if line[0] == ' ' and line[3] != '-':
                result += (line + '\n')
        return result
    
    def get_adsl_line_profile_board(self, board):
        """ Получить список активированных портов с платы """
        if board not in self.boards:
            return []
        regex = re.compile(r' +(\w*) +ADSL +Activat(ed|ing) +(\w*)')
        command_line = 'display board 0/{}'.format(board)
        str_out = self.write_read_data(command_line)
        if str_out is False:
            return False
        result = ['-' for x in range(0, self.ports)]
        for line in str_out.split('\n'):
            match = regex.search(line)
            if match:
                result[int(match.group(1))] = int(match.group(3))
        return result       

    def get_time(self):
        """ Получить Дату - Время с DSLAM """
        command = 'display time'
        str_out = self.write_read_data(command)
        if str_out is False:
            return False
        list_date = re.search(r'(\w{4}-\w{2}-\w{2})', str_out).group(1).split('-')
        list_time = re.search(r'(\w{2}:\w{2}:\w{2})', str_out).group(1).split(':')
        return datetime.datetime(int(list_date[0]), int(list_date[1]), int(list_date[2]), int(list_time[0]), int(list_time[1]), int(list_time[2]))
    
    def set_activate_port(self, board, port):
        """ Активировать порт """
        if (board not in self.boards) or (port not in range(0, self.ports)):
            return False        
        self.write_read_data('config',  short=True)
        self.write_read_data('interface adsl 0/{}'.format(board),  short=True)
        self.write_read_data('activate {}'.format(port))
        self.write_read_data('quit',  short=True)
        self.write_read_data('quit',  short=True)
    
    def set_deactivate_port(self, board, port):
        """ Деактивировать порт """
        if (board not in self.boards) or (port not in range(0, self.ports)):
            return False         
        self.write_read_data('config',  short=True)
        self.write_read_data('interface adsl 0/{}'.format(board),  short=True)
        self.write_read_data('deactivate {}'.format(port))
        self.write_read_data('quit',  short=True)
        self.write_read_data('quit',  short=True)
        
    def set_adsl_line_profile_port(self, board, port, profile_index):
        """ Изменить профайл на порту """
        if (board not in self.boards) or (port not in range(0, self.ports)):
            return False         
        if profile_index not in self.adsl_line_profile:
            return False  
        self.write_read_data('config',  short=True)
        self.write_read_data('interface adsl 0/{}'.format(board),  short=True)
        self.write_read_data('deactivate {}'.format(port))
        self.write_read_data('activate {} profile-index {}'.format(port, profile_index))
        self.write_read_data('quit',  short=True)
        self.write_read_data('quit',  short=True)
        
    def execute_command(self, command, short=False):
        command_line = command.strip()
        str_out = self.write_read_data(command, short)
        if str_out is False:
            return False
        return str_out
    
    def add_user(self, user_profile, user_name, user_password):
        self.tn.sendline('interactive')
        self.tn.expect('#')
        # Создание профайла для пользователя
        self.tn.sendline('terminal user-profile add')
        self.tn.expect('User profile name\(<=15 chars\):')
        self.tn.sendline(user_profile)
        num = self.tn.expect(['Validity period of the user name\(0--999 days\)\[0\]:', 'The user-profile has existed\.'])
        if num == 1:
            print('На DSLAM {} user-profile {} уже существует.'.format(self.hostname, user_profile))
            self.tn.expect('User profile name\(<=15 chars\)')
            self.tn.sendline('')
            self.tn.expect('User profile name\(<=15 chars\)')
            self.tn.sendline('')
            self.tn.expect('#')  
            self.tn.sendline('save')
            self.tn.expect('#') 
            return False
        if num == 0:
            self.tn.sendline('')
            self.tn.expect('Validity period of the password\(0--999 days\)\[0\]:')
            self.tn.sendline('')
            self.tn.expect('Permitted start time of logon by a user\(hh:mm\)\[00:00\]:')
            self.tn.sendline('')
            self.tn.expect('Permitted end time of logon by a user\(hh:mm\)\[00:00\]:')
            self.tn.sendline('')
            self.tn.expect('Repeat this operation\? \(y/n\)\[n\]:')
            self.tn.sendline('n')
            self.tn.expect('#')
            
            # Создание пользователя
            self.tn.sendline('terminal user name')
            self.tn.expect('User Name\(length<6,15>\):')
            self.tn.sendline(user_name)
            self.tn.expect('User Password\(length<6,15>\):')
            self.tn.sendline(user_password)
            self.tn.expect('Confirm Password\(length<6,15>\):')
            self.tn.sendline(user_password)
            self.tn.expect('User profile name\(<=15 chars\)\[root\]:')
            self.tn.sendline(user_profile)
            self.tn.expect('1. Common User  2. Operator  3. Administrator:')
            self.tn.sendline('2')
            self.tn.expect('Permitted Reenter Number\(0--4\):')
            self.tn.sendline('1')
            self.tn.expect('User\'s Appended Info\(<=30 chars\):')
            self.tn.sendline('Script User')          
            self.tn.expect('Repeat this operation\? \(y/n\)\[n\]:')
            self.tn.sendline('n')
            self.tn.expect('#')
            self.tn.sendline('undo interactive')
            self.tn.expect('#')
            self.tn.sendline('save')
            self.tn.expect('#')
            return True
            
            
class DslamHuawei5600(DslamHuawei):
    """ Huawei MA5600 """
    def __init__(self, ip, login, password, timeout=30):
        super().__init__(ip, login, password,  timeout)
        self.ports = 64
        self.model = '5600'
        self.set_boards()
    
    def set_boards(self):
        boards_list =  [0, 1, 2, 3, 4, 5, 6, 9, 10, 11, 12, 13, 14, 15]
        super().set_boards(boards_list)

            

class DslamHuawei5616(DslamHuawei):
    """ Huawei MA5616 """
    def __init__(self, ip, login, password,  timeout=30):
        super().__init__(ip, login, password,  timeout)
        self.ports = 32
        self.model = '5616'
        self.set_boards()
    
    def set_boards(self):
        boards_list =  [1, 2, 3, 4]
        super().set_boards(boards_list)