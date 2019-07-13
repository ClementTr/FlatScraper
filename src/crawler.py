from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from pyvirtualdisplay import Display
from selenium import webdriver
from io import StringIO
import pandas as pd
import numpy as np
import datetime
import boto3
import time
import sys
import re
import os

PAGE_MAX = int(os.environ['PAGE_MAX'])
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
DESTINATION = 'seloger-dump'
BASE_URL = "https://www.seloger.com/list.htm?enterprise=0&natures=1,2,4&picture=15&places=%5b%7bcp%3a75%7d%5d&\
price=240000%2f350000&projects=2&qsversion=1.0&surface=20%2fNaN&types=1&LISTING-LISTpg="

def get_cleaned_criterions(criterions):
    cleaned_criterions = []
    for criterion in criterions:
        cleaned_criterions.append([''.join(x) for x in zip(criterion.text.split()[0::2],
                                                           criterion.text.split()[1::2])])
    return cleaned_criterions

def get_criterions_df(cleaned_criterions):
    sizes = []
    rooms = []
    ascs = []
    bedrooms = []
    levels = []

    for criterions in cleaned_criterions:
        is_room = False
        is_size = False
        is_asc = False
        is_bedroom = False
        is_level = False
        for criterion in criterions:
            if "p" in criterion:
                is_room = criterion
            elif "m²" in criterion:
                is_size = criterion
            elif "asc" in criterion:
                is_asc = criterion
            elif "ch" in criterion:
                is_bedroom = criterion
            elif "etg" in criterion:
                is_level = criterion
        if is_room:
            rooms.append(is_room)
        else:
            rooms.append(None)
        if is_size:
            sizes.append(is_size)
        else:
            sizes.append(None)
        if is_asc:
            ascs.append(is_asc)
        else:
            ascs.append(None)
        if is_bedroom:
            bedrooms.append(is_bedroom)
        else:
            bedrooms.append(None)
        if is_level:
            levels.append(is_level)
        else:
            levels.append(None)

    df = pd.DataFrame({'sizes': sizes, 'rooms': rooms, 'ascs': ascs, 'bedrooms': bedrooms, 'levels': levels})
    return df

def get_prices(prices):
    apt_prices = [re.findall(r"\d\d\d|\d\d", p.text)[0] for p in prices]
    return apt_prices

def get_locations(locations):
    apt_locations = [l.text for l in locations]
    return apt_locations

def get_loans(loans):
    apt_loans = [l.text.split('ou ')[1] for l in loans]
    return apt_loans

def get_ids(ids):
    apt_ids = [sel_id.get_attribute('id') for sel_id in ids]
    return apt_ids

def clean_criterions(size, split):
    if size:
        value = size.split(split)[0]
        return float(value.replace(" ", "").replace(",", "."))
    return np.nan

def chrome_function():
    display = Display(visible=0, size=(800, 600))
    display.start()

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_experimental_option('prefs', {
        'download.default_directory': os.getcwd(),
        'download.prompt_for_download': False,
    })

    #browser = webdriver.Chrome(chrome_options=chrome_options)
    #browser.get(BASE_URL)
    #browser.quit()
    crawler(chrome_options)

    display.stop()

def write_dataframe_to_csv_on_s3(dataframe, filename):
    """ Write a dataframe to a CSV on S3 """
    print("Writing {} records to {}".format(len(dataframe), filename))
    # Create buffer
    csv_buffer = StringIO()
    # Write dataframe to buffer
    dataframe.to_csv(csv_buffer, sep="|", index=False)
    # Create S3 object
    s3_resource = boto3.resource("s3",
                                aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # Write buffer to S3 object
    s3_resource.Object(DESTINATION, filename).put(Body=csv_buffer.getvalue())

def crawler(chrome_options):
    browser = webdriver.Chrome(chrome_options=chrome_options)
    list_df_apts = []
    criterions = ["OK"]
    cpt = 1

    while(len(criterions)):
        browser.get(BASE_URL + str(cpt))
        time.sleep(2)
        criterions = browser.find_elements_by_class_name('c-pa-criterion')
        cleaned_criterions = get_cleaned_criterions(criterions)
        df_apts = get_criterions_df(cleaned_criterions)
        prices = browser.find_elements_by_class_name('c-pa-cprice')
        locations = browser.find_elements_by_class_name('c-pa-city')
        loans = browser.find_elements_by_class_name('c-pa-loan')
        ids = browser.find_elements_by_class_name('c-pa-list')
        df_apts["prices"] = get_prices(prices)
        df_apts["locations"] = get_locations(locations)
        df_apts["loans"] = get_loans(loans)
        df_apts["ids"] = get_ids(ids)
        print("Page {} done".format(cpt))
        list_df_apts.append(df_apts)
        cpt += 1
        if cpt >= PAGE_MAX:
            break;

    browser.quit()
    df_full = pd.concat(list_df_apts)

    now = datetime.datetime.now()
    date = str(now.day) + "-" + str(now.month)
    df_full = pd.concat(list_df_apts)
    df_full["prices"] = df_full["prices"].astype(float)
    df_full["ascs"] = df_full["ascs"]*1
    df_full["sizes"] = df_full["sizes"].apply(lambda x: clean_criterions(x, 'm²'))
    df_full["rooms"] = df_full["rooms"].apply(lambda x: clean_criterions(x, 'p'))
    df_full["bedrooms"] = df_full["bedrooms"].apply(lambda x: clean_criterions(x, 'ch'))
    df_full["levels"] = df_full["levels"].apply(lambda x: clean_criterions(x, 'etg'))
    df_full["loans"] = df_full["loans"].apply(lambda x: clean_criterions(x, '€'))
    df_full["price_m²"] = df_full["prices"] / df_full["sizes"] * 1000
    df_full.to_csv('df_' + date + '.csv', index=False)
    write_dataframe_to_csv_on_s3(df_full, 'data_' + date)
    print("DONE")


if __name__ == '__main__':
    chrome_function()
    time.sleep(20)
