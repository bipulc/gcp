#! /usr/bin/env python

# Data Generation code using Faker

from faker import Faker
import json
import argparse
import os
import datetime
import time
import random
from google.cloud import pubsub_v1

# Collect command line arguments
parser = argparse.ArgumentParser()
parser.add_argument("-l", type=str, help="output file directory", required=False)
parser.add_argument("-f", type=str, help="output filename prefix", required=False)
parser.add_argument("-n", type=str, help="number of records to generate", required=True)
parser.add_argument("-d", type=str, help="destination. options - file|pubsub", required=True)
parser.add_argument("-p", type=str, help="GCP project id", required=False)
parser.add_argument("-t", type=str, help="pubsub topic name", required=False)

args = parser.parse_args()

dirname = args.l
filename = args.f
numrecords = int(args.n)
destination = args.d
project=args.p
topic = args.t

if destination not in ['file', 'pubsub']:
    print "destination should be file or pubsub"
    exit (1)

if destination == 'file' and ( dirname is None or filename is None ):
    print "Dirname and Filename must be specified if destination is file"
    exit (2)

if destination == 'pubsub' and ( topic is None or project is None):
    print "An existing pubsub topic name and project name must be specified if destination is pubsub"
    exit(3)

'''
Write output to file
'''
def writetofile (ofile):
    fake = Faker()
    fake.name()
    fake.address()
    for _ in range(numrecords):
        #print(fake.name(),fake.address(), fake.city())
        datum = {
                    "name":fake.name(),
                    "address":fake.address(),
                    "country_code":fake.country_code(),
                    "city":fake.city(),
                    "bank_iban":fake.iban(),
                    "company":fake.company(),
                    "credit_card":fake.credit_card_number(),
                    "credit_card_provider":fake.credit_card_provider(),
                    "credit_card_expires":fake.credit_card_expire(start="now", end="+10y",date_format="%m/%y"),
                    "date_record_created":fake.date(pattern="%Y-%m-%d %H:%M:%S", end_datetime=None),
                    "employement":fake.job(),
                    "emp_id":fake.uuid4(),
                    "name_prefix":fake.prefix(),
                    "phone_number":fake.phone_number()
                }
        # print datum
        # print json.dumps(datum)
        ofile.write(json.dumps(datum))
        ofile.write('\n')

'''
Publish messages to PubSub
'''
def writetopubsub (project, topic):

    # setup pubsub client and topic path
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)

    # generate some fake data
    fake = Faker()
    fake.name()
    fake.address()
    i = 0
    for _ in range(numrecords):
        #print(fake.name(),fake.address(), fake.city())
        datum = {
                    "name":fake.name(),
                    "address":fake.address(),
                    "country_code":fake.country_code(),
                    "city":fake.city(),
                    "bank_iban":fake.iban(),
                    "company":fake.company(),
                    "credit_card":fake.credit_card_number(),
                    "credit_card_provider":fake.credit_card_provider(),
                    "credit_card_expires":fake.credit_card_expire(start="now", end="+10y",date_format="%m/%y"),
                    "date_record_created":fake.date(pattern="%Y-%m-%d %H:%M:%S", end_datetime=None),
                    "employement":fake.job(),
                    "emp_id":fake.uuid4(),
                    "name_prefix":fake.prefix(),
                    "phone_number":fake.phone_number()
                }
        datum = json.dumps(datum)
        datum = datum.encode('utf-8')
        future = publisher.publish(topic_path, data = datum)
        # print (future.result())
        # Add some delay for every 100 records
        #i += 1
        #if i >= 100:
        #    time.sleep(1)
        #    i = 0

if destination == 'file':
    ofilename = os.path.join(dirname,filename+'.'+str(random.randint(1,10000)))
    ofile = open(ofilename,'w')
    writetofile(ofile)
    ofile.close()
elif destination == 'pubsub':
    writetopubsub(project, topic)
