import os, sys, email, re
import numpy as np
import pandas as pd
import json
from dateutil.parser import parse
import nltk
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
import string
from datetime import datetime

emails_df = pd.read_csv('./data/emails.csv', nrows=100)
# emails_df = pd.read_csv('./data/emails.csv') #Uncomment this line to read all the data
print(emails_df.shape)


# print(emails_df.head())


def get_content_from_email(msg):
    '''To read the email content from email objects'''
    content = []
    for item in msg.walk():
        if item.get_content_type() == 'text/plain':
            content.append(item.get_payload())
    return ''.join(content)


def split_email_addresses(line):
    '''To separate multiple email addresses'''
    if line:
        addresses = line.split(',')
        addresses = list(set(map(lambda x: x.strip(), addresses)))
    else:
        addresses = None
    return addresses


messages = list(map(email.message_from_string, emails_df['message']))
emails_df.drop('message', axis=1, inplace=True)

key_headers = messages[0].keys()
key_headers.append('content')

for header in key_headers:
    emails_df[header] = [item[header] for item in messages]

emails_df['content'] = list(map(get_content_from_email, messages))
emails_df['From'] = emails_df['From'].map(split_email_addresses)
emails_df['To'] = emails_df['To'].map(split_email_addresses)
print('---------------')

sender_receiver = {}
key_headers.insert(0, 'file')
i = 0
for row in emails_df.itertuples(index=False):
    try:
        row = row._asdict()
        row = dict(zip(key_headers, row.values()))
        sender = str(row['From'][0])
        receivers = list(row['To'])

        for receiver in receivers:
            key = (sender, receiver.replace("\n", ""))
            key = str(key)
            if key not in sender_receiver.keys():
                sender_receiver[key] = [row]
            else:
                sender_receiver[key].append(row)
        i += 1
        if i % 50000 == 0:
            print("Processed {} records! ".format(i))
    except(RuntimeError, ValueError, TypeError):
        # print("Error item is : ", items)
        with open("error.txt", "a") as text_file, open("raw_files.txt", "a") as raw_file:
            text_file.write("for line : {} , Error item is : {}\n".format(i, row))
            raw_file.write("{}\n".format(str(row['file'])))
    print("Processed {} records! ".format(i))

with open('sender_receiver_pd.json', 'w') as fp:
    json.dump(sender_receiver, fp)
print("Result saved to Json file")


def check_messages_sum(sender, receiver, from_date, to_date):
    total_messages = 0
    key = (sender, receiver)
    key = str(key)
    print(key)
    if key in sender_receiver.keys():
        messages = sender_receiver[key]
        for msg in messages:
            if from_date <= parse(msg['Date']).date() <= to_date:
                total_messages += 1
    else:
        print("They never communicated.")
    print(" Total exchanged messages between {} and {} is : {}".format(sender, receiver, total_messages))
    return total_messages


sndr = 'phillip.allen@enron.com'
rcvr = 'john.lavorato@enron.com'
from_date = parse('5 Sep 2000').date()
to_date = parse('4 May 2001').date()
num_msgs = check_messages_sum(sndr, rcvr, from_date, to_date)
print(num_msgs)

# sorted the list of dictionaries based on email date which will help later
# in deciding different timeperiods
for item in sender_receiver.keys():
    sender_receiver[item] = sorted(sender_receiver[item], key=lambda x: parse(x['Date']))


# Code for cleaning the content part of the email so that we can do topic modeling
def clean_data(text):
    stop = set(stopwords.words('english'))
    stop.update(("to", "cc", "subject", "http", "from", "sent",
                 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
    exclude = set(string.punctuation)
    lemma = WordNetLemmatizer()

    text = text.rstrip()
    text = re.sub(r'[^a-zA-Z]', ' ', text)
    stop_free = " ".join([i for i in text.lower().split() if ((i not in stop) and (not i.isdigit()))])
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
    normalized = " ".join(lemma.lemmatize(word) for word in punc_free.split())

    return normalized


# List of unnecessary parts of email message
removable_data = ['Mime-Version', 'Content-Transfer-Encoding', 'Content-Type', 'X-FileName', 'X-Origin', 'X-Folder']

for k, v in sender_receiver.items():
    for msg in v:
        msg['content'] = clean_data(msg['content'])
        for item in removable_data:
            msg.pop(item, None)

with open('clean_data.json', 'w') as fps2:
    json.dump(sender_receiver, fps2)


# Selecting only those pairs who have more than 5 messages exchanged between them.
filtered_data = {}
print("printing the pair name which we will consider ... ")
for k, v in sender_receiver.items():
    if len(v) > 5:
        filtered_data[k] = v
        print(k)

with open('test_file.json', 'w') as fpq:
    json.dump(filtered_data, fpq)
