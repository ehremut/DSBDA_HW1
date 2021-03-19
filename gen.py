from random import randrange
from datetime import timedelta
from datetime import datetime
import random
import re


def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

warnings = ['0', '1', '2', '3', '4', '5', '6', '7']

date_regular = "[1-2][0-9]{3}\-(0[1-9]|1[0-2])\-(0[1-9]|1[0-9]|2[0-9]|3[0-1])\ (0[0-9]|1[0-9]|2[0-3])(\:(0[0-9]|[1-5][0-9])){2}"
date_format = '%Y-%m-%d %H:%M:%S'
error_date_mess = "it's wrong date format"

start = input('input start date like 2021-01-01 03:12:23): ')
if re.match(date_regular, start) == None:
    print(error_date_mess)
    exit(1)

end = input('input end date like 2021-12-01 03:12:23: ')
if re.match(date_regular, end) == None:
    print(error_date_mess)
    exit(1)

how = input('how many entries per day do you need? ')
if int(how) == None:
    print("wrong number")

d1 = datetime.strptime(start, date_format)
d1_cpy = d1.replace(hour=23, minute=59, second=59)
d2 = datetime.strptime(end, date_format)
qty_files = d2.day - d1.day
f = open("input_file", 'wb')
for i in range(qty_files + 1):
    for j in range(int(how)):
        if random.choice(warnings) != warnings[0]:
            war_res = str(random.choice(warnings))
            time_res = str(random_date(d1, d1_cpy))
            result_str = time_res + ',' + war_res + '\n'
            f.write(str.encode(result_str))
        else:
            f.write(b'DSBDA-2021\n')
            f.write(b'03/05/df2021 05\n')
            f.write(b'0d5:00:00\n')
    d1 = d1.replace(day=d1.day + 1, hour=0, minute=0, second=0)
    if i != qty_files - 1:
        d1_cpy = d1_cpy.replace(day=d1_cpy.day + 1)
    else:
        d1 = d2.replace(hour=0, minute=0, second=0)
        d1_cpy = d2
f.close()
