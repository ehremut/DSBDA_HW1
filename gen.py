from random import randrange
from datetime import timedelta
from datetime import datetime
import random
import re

# gen random date
def random_date(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

# list of warnings number
warnings = ['0', '1', '2', '3', '4', '5', '6', '7']

# regular for date
date_regular = "[1-2][0-9]{3}\-(0[1-9]|1[0-2])\-(0[1-9]|1[0-9]|2[0-9]|3[0-1])\ (0[0-9]|1[0-9]|2[0-3])(\:(0[0-9]|[1-5][0-9])){2}"

# date format
date_format = '%Y-%m-%d %H:%M:%S'

#error message
error_date_mess = "it's wrong date format"


# input start date
start = input('input start date like 2021-01-01 03:12:23): ')

# check if error
if re.match(date_regular, start) == None:
    print(error_date_mess)
    exit(1)

# input end date
end = input('input end date like 2021-12-01 03:12:23: ')

# check if error
if re.match(date_regular, end) == None:
    print(error_date_mess)
    exit(1)

#input count of writes in day
how = input('how many entries per day do you need? ')
if int(how) == None:
    print("wrong number")
    
chance = int(input('enter chance of error like number from 0 to 100: '))
if chance > 100 or chance < 0:
    print("wrong format")
    exit(1)    
    
# get start date in datetime
d1 = datetime.strptime(start, date_format)

# get end of start date
d1_cpy = d1.replace(hour=23, minute=59, second=59)

# get end date
d2 = datetime.strptime(end, date_format)

# get count of days
qty_files = d2.day - d1.day

# create and open file for write
f = open("input_file", 'wb')
# cycle in count of days
for i in range(qty_files + 1):
    # cycle in write count in dat
    for j in range(int(how)):
        # with a probability of 90% create good write
        if random.randrange(0, 100, 1) > chance:
            war_res = str(random.choice(warnings))
            time_res = str(random_date(d1, d1_cpy))
            result_str = time_res + ',' + war_res + '\n'
            f.write(str.encode(result_str))
        # with a probability of 10% create good write    
        else:
            f.write(b'DSBDA-2021\n')
            f.write(b'03/05/df2021 05\n')
            f.write(b'0d5:00:00\n')
    #go on next day
    d1 = d1.replace(day=d1.day + 1, hour=0, minute=0, second=0)
    if i != qty_files - 1:
        d1_cpy = d1_cpy.replace(day=d1_cpy.day + 1)
    else:
        d1 = d2.replace(hour=0, minute=0, second=0)
        d1_cpy = d2
#close file
f.close()
