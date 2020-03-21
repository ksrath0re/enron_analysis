import os, sys, email, re
import numpy as np
import pandas as pd
import json

emails_df = pd.read_csv('./data/emails.csv', nrows=100)
print(emails_df.shape)
print(emails_df.head())


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
for row in emails_df.itertuples(index=False):
    row = row._asdict()
    row = dict(zip(key_headers, row.values()))
    print(row)
    sender = str(row['From'][0])
    receivers = list(row['To'])

    for receiver in receivers:
        key = (sender, receiver.replace("\n", ""))
        key = str(key)
        if key not in sender_receiver.keys():
            sender_receiver[key] = [row]
        else:
            sender_receiver[key].append(row)

with open('sender_receiver_pd.json', 'w') as fp:
    json.dump(sender_receiver, fp)
print("Result saved to Json file")
