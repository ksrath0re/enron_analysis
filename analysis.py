import csv, sys
from itertools import islice
from subprocess import check_output
import json
import re
csv.field_size_limit(sys.maxsize)
print(check_output(["ls", "./data/"]).decode("utf8"))


def read_data(file):
    with open(file, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file)
        data = []
        line_count = 0
        #for row in islice(csv_reader, 10):
        for row in csv_reader:
            data.append(row)
            line_count += 1

    print("Lines processed : ", line_count)
    del data[0]
    return data


def list_to_dict(input_list, message):
    dic = dict(e.split(':', 1) for e in input_list)
    dic['Message-Content'] = message
    return dic


data_from_csv = read_data("./data/emails.csv")
print(len(data_from_csv))
sender_receiver = {}
for i, line in enumerate(data_from_csv):
    items = line[1].split("\n", 15)
    sender = items[2].split(":")[1].strip()
    receivers_list = items[3].split(":")[1]
    message = items[15]
    for k in [15, 3, 2]:
        del items[k]
    if len(receivers_list.split(",")) > 0:
        #receivers_list = [j.strip('\t') for j in receivers_list]
        receivers = receivers_list.split(",")
    else:
        receivers = [receivers_list]
    try:
        for receiver in receivers:
            key = (sender, receiver.replace("\n", ""))
            key = str(key)
            if key not in sender_receiver.keys():
                sender_receiver[key] = [list_to_dict(items, message)]
            else:
                sender_receiver[key].append(list_to_dict(items, message))
    except(RuntimeError, ValueError):
        #print("Error item is : ", items)
        with open("error.txt", "a") as text_file, open("raw_files.txt", "a") as raw_file:
            text_file.write("for line : {} , Error item is : {}\n".format(i, items))
            raw_file.write("{}\n".format(str(line[0])))


print('--------')

#print(sender_receiver)
with open('sender_receiver.json', 'w') as fp:
    json.dump(sender_receiver, fp)
print("Result saved to Json file")
