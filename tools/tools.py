import os
from datetime import datetime


def logging(text):
    with open(f'{os.getcwd() + os.sep}log.txt', 'a', encoding='utf8') as log_file:
        for log_text in text.split('\n'):
            message = f'{datetime.now()} :   {log_text}'
            print(message)
            log_file.write(f'{message}\n')