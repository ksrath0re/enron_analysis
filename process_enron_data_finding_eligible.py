import os, sys, email, re
import numpy as np
import pandas as pd
import json
from dateutil.parser import parse
from ast import literal_eval as make_tuple
import nltk
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
import string
from datetime import datetime
import gensim
import pyLDAvis.gensim as gensimvis
import pyLDAvis
from IPython.core.display import HTML
#nltk.download('all')
#nltk.download('averaged_perceptron_tagger')
emails_df = pd.read_csv('data/emails.csv', nrows=1000)
#emails_df = pd.read_csv('./emails.csv') #Uncomment this line to read all the data
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
    #print("Processed {} records! ".format(i))

# with open('sender_receiver_pd.json', 'w') as fp:
#     json.dump(sender_receiver, fp)
# print("Result saved to Json file")


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
def clean_data(text, msg):
    stop = set(stopwords.words('english'))
    stop.update(("to", "cc", "subject", "http", "from", "sent", "allan", "allen", "can", "could", "will", "would", "shall", "should"
                 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
    names_to = msg["To"][0].split('@')[0].split('.')
    names_from = msg["From"][0].split('@')[0].split('.')
    names_from.extend(names_to)
    # Removing names appearing in the content
    #print("names : ", names_from)
    stop.update(names_from)
    stop.update(names_to)
    exclude = set(string.punctuation)
    lemma = WordNetLemmatizer()

    text = text.rstrip()
    text = re.sub(r'[^a-zA-Z]', ' ', text)
    stop_free = " ".join([i for i in text.lower().split() if ((i not in stop) and (not i.isdigit()) and (len(i) > 3))])
    punc_free = ''.join(ch for ch in stop_free if ch not in exclude)
    normalized = " ".join(lemma.lemmatize(word) for word in punc_free.split())

    return normalized


# List of unnecessary parts of email message
removable_data = ['Mime-Version', 'Content-Transfer-Encoding', 'Content-Type', 'X-FileName', 'X-Origin', 'X-Folder']

for k, v in sender_receiver.items():
    for msg in v:
        msg['content'] = clean_data(msg['content'], msg)
        msg['split-content'] = msg['content'].split()
        for item in removable_data:
            msg.pop(item, None)

with open('clean_data.json', 'w') as fps2:
    json.dump(sender_receiver, fps2)


# Selecting only those pairs who have more than 5 messages exchanged between them.
filtered_data = {}
print("printing the pair name which we will consider ... ")
for k, v in sender_receiver.items():
    if len(v) > 2:
        filtered_data[k] = v
        #print(k)

results = {}
# clean_text = []
# # for k, v in filtered_data.items():

for k, v in filtered_data.items():
    timeperiod_1 = []
    timeperiod_2 = []
    first_date = parse(v[0]['Date']).date()
    last_date = parse(v[-1]['Date']).date()
    mid_date = parse('12 Feb 2001').date()
    #print("mid date : ", mid_date)
    for msg in v:
        if parse(msg['Date']).date() < mid_date:
            timeperiod_1.extend(msg['split-content'])
        else:
            timeperiod_2.extend(msg['split-content'])
    results[k] = {'time_period_1' : timeperiod_1, 'time_period_2' : timeperiod_2}

# with open('results.json', 'w') as rs:
#     json.dump(results, rs)

# Old testing pairs
# k = "('phillip.allen@enron.com', 'ina.rangel@enron.com')"
# k = "('phillip.allen@enron.com', 'tim.belden@enron.com')"
# k = "('phillip.allen@enron.com', 'jacquestc@aol.com')"
# k = "('stephanie.panus@enron.com', 'brent.hendry@enron.com')"
# k = "('stephanie.panus@enron.com', 'brent.hendry@enron.com')"
# k = "('phillip.allen@enron.com', 'monique.sanchez@enron.com')"
# k = "('phillip.allen@enron.com', 'jane.tholt@enron.com')"
# k = "('phillip.allen@enron.com', 'steven.south@enron.com')"
# k = "('phillip.allen@enron.com', 'felix.buitron@enron.com')"


def topic_model_analysis(processed_mail, topic_file, timepeiod=1):
    dictionary = gensim.corpora.Dictionary(processed_mail)
    #print(dictionary)

    # count = 0
    # for k, v in dictionary.iteritems():
    #     print(k, v)
    #     count += 1
    #     if count > 50:
    #         break
    if len(dictionary) != 0:
        bow_corpus = [dictionary.doc2bow(mail) for mail in processed_mail]
        #print("bow corpus :  ", bow_corpus)

        bow_corpus_0 = bow_corpus[0]
        # for i in range(len(bow_corpus_0)):
        #     print("Word {} (\"{}\") appears {} time.".format(bow_corpus_0[i][0],
        #                                                      dictionary[bow_corpus_0[i][0]],
        #                                                      bow_corpus_0[i][1]))

        # Building LDA model for topic modeling
        lda_model = gensim.models.LdaMulticore(bow_corpus, num_topics=10, id2word=dictionary, passes=50, workers=2)
        # with open(topic_file, 'a') as tf:
        #     tf.write("For Time period {} \n---------------- \n".format(timepeiod))
        #     tokens = ', '.join(dictionary.values())
        #     tf.write("tokens : " + tokens + '\n')
        # for idx, topic in lda_model.print_topics(-1):
        #     print('Topic: {} \nWords: {}'.format(idx, topic))
        #     with open(topic_file, 'a') as tf:
        #         tf.write('Topic: {} \nWords: {}\n'.format(idx, topic))
        # with open(topic_file, 'a') as tf:
        #     tf.write('\n')
        # top_topic_terms = lda_model.get_topic_terms(idx, topn=10)
        # for wordId, val in top_topic_terms:
        #     print(dictionary[wordId], val)
        return lda_model, bow_corpus, dictionary
    else:
        return None, None, None


#person_pair = pair[0].split('@')[0] + '-' + pair[1].split('@')[0]
# topic_file = person_pair+'-topic-file.txt'
print("checking eligible names :")
for k in results.keys():
    pair = make_tuple(k)
    person_pair = pair[0].split('@')[0] + '-' + pair[1].split('@')[0]
    topic_file = person_pair + '-topic-file.txt'
    processed_mail_1 = [results[k]['time_period_1']]
    lda_for_1, corpus1, dic1 = topic_model_analysis(processed_mail_1, topic_file, timepeiod=1)
    processed_mail_2 = [results[k]['time_period_2']]
    lda_for_2, corpus2, dic2 = topic_model_analysis(processed_mail_2, topic_file, timepeiod=2)

    if dic1 is not None and dic2 is not None:
        # print(\" +k+\")
        print(f'"{k}"')

# doc_topic_dist = np.array()
# vis_data_1 = gensimvis.prepare(lda_for_1, corpus1, dic1, R=10)
# vis_data_2 = gensimvis.prepare(lda_for_2, corpus2, dic2, R=10)
# pyLDAvis.show(vis_data_2)