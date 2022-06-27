"""
Created on Mon 27 22:58:13 2022
@author: Ana Angélica da Costa Marchiori
"""

# --- LIBRARIES
import numpy as np
import pandas as pd
import requests
import logging
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3

# --- LOG SETTINGS
logging.basicConfig(filename = '../logs/log_sj.txt',
                    format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
                    datefmt = '%Y-%m-%d %H:%M:%S',
                    level = logging.INFO)

logger = logging.getLogger('log_sj')

# --- FUNCTIONS
def page_size(url, headers):
    # API request
    try:
        page = requests.get(url, headers=headers)
    except:
        logging.critical('Problem in requests - Function: page_size()')

    # transform the html request into a beautiful soup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # page size
    itens = soup.find_all('h2')[2]

    itens_shown = int(itens['data-items-shown'])
    total_itens = int(itens['data-total'])

    if itens_shown != 0:
        page_size = str(int(np.ceil(total_itens / itens_shown) * itens_shown))
    else:
        logging.error('Division by zero')

    # new url with the total amount of items
    new_url = 'https://www2.hm.com/en_us/men/products/jeans.html?sort=stock&image-size=small&image=model&offset=0&page-size=' + page_size

    return new_url


def item_url(url, headers):
    # API request
    try:
        page = requests.get(url, headers=headers)
    except:
        logging.critical('Problem in requests - Function: item_url()')

    # transform the html request into a beautiful soup object
    soup = BeautifulSoup(page.text, 'html.parser')

    # identifies the products
    product_item_li = soup.find_all('li', class_='product-item')

    # extract the details url
    domain = 'https://www2.hm.com'
    item_url = [domain + i.find('a')['href'] for i in product_item_li]

    return item_url


def colors_url(url, headers):
    # creates the dataframe structure
    color_info = pd.DataFrame(columns=['color_url', 'color_id', 'color_name'])

    for u in url:
        # API request
        try:
            page = requests.get(u, headers=headers)
        except:
            logging.critical('Problem in requests - Function: colors_url()')

        # transform the html request into a beautiful soup object
        soup = BeautifulSoup(page.text, 'html.parser')

        # collects information about colors
        info = soup.find_all('a', class_='filter-option')

        # auxiliary dataframe
        aux = pd.DataFrame(columns=['color_url', 'color_id', 'color_name'])

        domain = 'https://www2.hm.com'

        aux['color_url'] = [domain + i['href'] for i in info]
        aux['color_name'] = [i['title'] for i in info]
        aux['color_id'] = [i['data-articlecode'] for i in info]

        # contacts collected informations in a single dataframe
        color_info = pd.concat([color_info, aux], ignore_index=True)

    # drop duplicates
    color_info = color_info.drop_duplicates(keep='first').reset_index(drop=True)

    return color_info


def details(url, headers, color_info):
    # empty dataframe
    df_details = pd.DataFrame()

    cols = ['Art. No.', 'Composition', 'Fit', 'Product safety', 'Size']
    df_pattern = pd.DataFrame(columns=cols)

    for url in color_info.loc[:, 'color_url']:
        # API request
        try:
            page = requests.get(url, headers=headers)
        except:
            logging.critical('Problem in requests - Function: details()')

        # transform the html request into a beautiful soup object
        soup = BeautifulSoup(page.text, 'html.parser')

        # price and name
        info1 = soup.find('div', class_='inner')
        name = info1.find('h1').text
        price = info1.find('span', class_='price-value').text

        # product features
        info2 = soup.find('div', class_='details parbase')
        aux2 = [list(filter(None, i.get_text().split('\n'))) for i in info2.find('dl').find_all('div')]

        # index
        line = color_info[color_info['color_url'] == url].index

        # put the data in the dataframe color_info
        color_info.loc[line, 'name'] = name
        color_info.loc[line, 'price'] = price
        color_info['web_scraping_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # reaname dataframe
        aux2 = pd.DataFrame(aux2).T
        aux2.columns = aux2.iloc[0]

        # delete first row
        aux2 = aux2.iloc[1:].fillna(method='ffill')

        # garantee the same number of columns
        aux2 = pd.concat([df_pattern, aux2], axis=0)

        # all details products
        df_details = pd.concat([df_details, aux2], axis=0)

    # reset index
    df_details = df_details.reset_index(drop=True)

    # merge
    df = df_details.merge(color_info, left_on='Art. No.', right_on='color_id')

    return df


def data_cleaning(data):
    # delete $ from records
    data['price'] = data['price'].apply(lambda x: x.replace('$', '')).str.strip()

    # composition
    data = data[~data['Composition'].str.contains('Pocket lining:', na=False)]
    data = data[~data['Composition'].str.contains('Lining:', na=False)]
    data = data[~data['Composition'].str.contains('Pocket:', na=False)]

    # reset index
    data = data.reset_index(drop=True)

    # formats the values of the variable Composition
    for i in data[data['Composition'].str.contains('Shell:', na=False)]['Composition']:
        # index
        line = data[data['Composition'] == i].index

        # extract only the compositon
        data.loc[line, 'Composition'] = i.split(': ')[1]

    # change data type - id
    data['Art. No.'] = data['Art. No.'].astype(int)

    # change data type - price
    data['price'] = data['price'].astype(float)

    # change data type - date
    data['web_scraping_date'] = pd.to_datetime(data['web_scraping_date'], format='%Y-%m-%d %H:%M:%S')

    # select features
    data = data[['Art. No.', 'Composition', 'Fit', 'color_url', 'color_name', 'name', 'price', 'web_scraping_date']]

    # rename
    data = data.rename(
        columns={'Art. No.': 'id', 'Composition': 'composition', 'color_url': 'url', 'color_name': 'color',
                 'web_scraping_date': 'date', 'Fit': 'fit'})

    return data


def data_insert(data):
    query_jeans_schema = """
                            CREATE TABLE IF NOT EXISTS jeans(
                                    id               INTEGER,
                                    composition      TEXT,
                                    fit              TEXT,              
                                    url              TEXT,
                                    color            TEXT,
                                    name             TEXT,
                                    price            REAL,
                                    date             TEXT
                            );
                    """

    # create table
    conn = sqlite3.connect('../data/database_jeans.sqlite')
    cursor = conn.execute(query_jeans_schema)
    conn.commit()

    # organize the table
    data_insert = data[['id', 'date', 'name', 'price', 'fit', 'composition', 'color', 'url']].copy()

    # insert data
    data_insert.to_sql('jeans', con=conn, if_exists='append', index=False)

    # close
    conn.close()

# --- MAIN
if __name__ == "__main__":
    # simulates a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebkit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    # home page
    url = 'https://www2.hm.com/en_us/men/products/jeans.html'

    new_url = page_size(url, headers)
    collection = item_url(new_url, headers)
    colors = colors_url(collection, headers)
    det = details(url, headers, colors)
    dc = data_cleaning(det)
    data_insert(dc)
    logging.info('The software ran successfully')