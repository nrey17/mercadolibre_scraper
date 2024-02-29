
# Mercadolibre API Scrapper

import requests
import pandas as pd
from datetime import datetime
from IPython.display import display
import sys
# import re
# import logging
# import json
from sys import argv
from os import mkdir, listdir
from os.path import dirname, abspath, join, exists
from os.path import isdir, isfile, basename
from time import time, strftime, sleep
from math import ceil

from multiprocessing import cpu_count
import multiprocessing
from concurrent.futures import wait
from concurrent.futures import ALL_COMPLETED
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from hashlib import md5
from queue import Queue
from threading import Thread
from json import loads, dumps
import json

from smtplib import SMTP
# from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

#import redis
import pandas as pd
import numpy as np
from fake_useragent import UserAgent
import sys
import random
import itertools
import pickle

pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 3000)
pd.set_option('display.colheader', 'justify')
pd.set_option('display.precision', 3)
pd.set_option('display.max_colwidth', None)

a = UserAgent(verify_ssl=False)


#------PERSISTANCE VARIABLES--------
Last_Category_ID = 'last_category_id'
Last_Category_Link = 'last_category_link'
#------ MONGO VARIABLES-------- 
MongoDB_IP = "localhost"
MongoDB_Port = 27017
MongoDB_DB = 'crawler_meli_db'
#------------REDIS-----------
Redis_IP = "localhost"
Redis_Port = 6379
Redis_DB = 10
Redis_PWD = ''
#----------SEATALK API---------
Seatalk_API = 'https://urldefense.proofpoint.com/v2/url?u=https-3A__openapi.seatalk.io_webhook_group_wcJIRHdoS2uZEPJssQR-5FYA&d=DwIGAw&c=R1GFtfTqKXCFH-lgEPXWwic6stQkW4U7uVq33mt-crw&r=Z9X0DC7BdOVFiX7yirGE_CXiyq5QmQlwCutBmwb3Wy8&m=fXVwLt_XogKxoHbCNCB4-NlcFgdk0AV45KNmZNeR5V4hFHV1zGEwil1_QcLLyPAU&s=Q7An-zgINrMIrQKc9RzFslNKEsvGKgcIKyxuzgPP0SU&e= '
#-------METHODS VARIABLES---------
categories_df = pd.DataFrame(
    columns = [
        'site_id',
        'category_1_id', 
        'category_1', 
        'category_2_id', 
        'category_2',
        'category_3_id', 
        'category_3'
        ])
        
output_df = pd.DataFrame({
    'id': [],
    'site_id': [],
    'category_id': [],
    'category_1_id': [], 
    'category_1_name': [], 
    'category_2_id': [],
    'category_2_name': [],
    'title': [],
    'price': [],
    'original_price': [],
    'available_quantity': [],
    'sold_quantity': [],
    'condition': [],
    'seller.id': [],
    'nickname': [], 
    'registration_date': [], 
    'points': [], 
    'address_city': [], 
    'address_state': [], 
    'seller_level_id':[], 
    'power_seller_status':[],
    'seller_transactions_canceled':[], 
    'seller_transactions_completed':[], 
    'seller_ratings_negative':[], 
    'seller_ratings_neutral':[], 
    'seller_ratings_positive':[], 
    'seller_transactions_total':[], 
    'seller_site_status': [],
    'permalink': [],
    'created_at': []
})
site_id = ['MLC']
pagination = [0, 51, 101, 151, 201, 251, 301, 351, 401, 451, 501, 551, 601, 651, 701, 751, 801, 851, 901, 951]


def callback(thread):
    exception = thread.exception()
    if exception:
        print(exception)

def send_msg(msg, url=Seatalk_API, at_all=False, mention_list=None):
    """
    向seatalk群机器人发送消息
    :param url:
    :param msg:
    :param at_all:
    :param mention_list:
    :return:
    """
    if not isinstance(mention_list, list):
        mention_list = []
    headers = {'content-type': 'application/json'}
    data = dumps({
        "tag": "text",
        "text": {
            "content": msg,
            "at_all": at_all,
            "mentioned_email_list": mention_list
        }
    })
    requests.post(url=url, data=data, headers=headers)

if sys.version_info[0]==2:
    import six
    from six.moves.urllib import request
    import random
    username = 'lum-customer-c_f3c5b911-zone-data_center-route_err-pass_dyn'
    password = '3t3bv1kjk20x'
    port = 22225

if sys.version_info[0]==3:
    import urllib.request
    import random
    username = 'lum-customer-c_f3c5b911-zone-data_center-route_err-pass_dyn'
    password = '3t3bv1kjk20x'
    port = 22225
    

def get_proxy_session():
    session_id = str(random.random())
    super_proxy_url = ('http://%s-country-co-session-%s:%s@zproxy.lum-superproxy.io:%d' %(username, session_id, password, port))
    proxy_urls= {
            'http': super_proxy_url,
            'https': super_proxy_url,
            }
    return proxy_urls

##----------------Vatiables fo product api------------------------------------------
region = 'CO'
category=3
first_run=False
data_batch = strftime('%Y%m')
item_table = f'meli_{region.lower()}_item_batch{data_batch}_category{category}'

##----------------Vatiables fo product api------------------------------------------

def request_product_api(tuple):
    global count, ready_list, no_items_list
    id,offset=tuple
    #print(offset)
    proxy=get_proxy_session()
    r = requests.get("https://api.mercadolibre.com/sites/MCO/search?category={}&offset={}".format(id, offset),proxies=proxy)
    #print(r.status_code)
    if r.status_code==200:
        products_json = json.loads(r.text)
        products_df = pd.json_normalize(products_json["results"])
        if len(products_df.index) > 0:
            products_df.to_csv("output/products_{}_{}.csv".format(id,offset), index = False)
            ready_list.append(id)
            with open("ready_list.pkl", "wb") as fp:   #Pickling
                pickle.dump(ready_list, fp)
            
            print(f'done {id} offset {offset}')
        else:

            no_items_list.append(id)
            with open("no_items_list.pkl", "wb") as fp:   #Pickling
                pickle.dump(no_items_list, fp)
            
    else:
        print(f'request_product_api failed id {id} offset {offset}')
    
    print(count)
    count = count + 1
    #return (f'done {id} offset {offset}')

def try_twice(tuple):
    try:
        request_product_api(tuple)
    except:
        try:
            request_product_api(tuple)
        except:
            pass

def run_item_process(total_ids, pagination, item_table=item_table, first_run=True):

    duples=itertools.product(total_ids,pagination)

    print("NR: TOTAL DUPLAS", len(total_ids) * len(pagination))
    with ThreadPoolExecutor(max_workers=50) as executor:
        result_pool = list(
                executor.map(try_twice, duples),
            )


def auto_run(first_run,category):
    global count, ready_list, no_items_list
    count = 1

    with open("total_pickle.pkl", "rb") as fp:   # Unpickling
        total_ids = pickle.load(fp)

    try:
        with open("ready_list.pkl", "rb") as fp:   # Unpickling
            ready_list = pickle.load(fp)
        total_ids = set(total_ids) - set(ready_list)
    except:
        ready_list = list([])
    try:   
        with open("no_items_list.pkl", "rb") as fp:   # Unpickling
            no_items_list = pickle.load(fp)
        total_ids = list(set(total_ids) - set(no_items_list))
    except:
        no_items_list = list([])
    print(total_ids)
    print(len(total_ids))
    run_item_process(total_ids,pagination)
        
    
if __name__ == '__main__':
    

    auto_run(first_run,category)


