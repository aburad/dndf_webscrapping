# -*- coding: utf-8 -*-
"""
Created on Sat Jul 25 01:57:35 2020

@author: Ajit
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Jul 24 18:59:03 2020

@author: Ajit
"""
from selenium import webdriver
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date

indo_months = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'] 
id_prefix="ctl00_PlaceHolderMain_biwebQueryContent_ListViewArtikelHits_ctrl"
id_suffix="_HyperLinkJudul"
date_prefix="ctl00_PlaceHolderMain_biwebQueryContent_ListViewArtikelHits_ctrl"
date_suffix="_lblNewsDate"
URL="https://www.bi.go.id/id/moneter/operasi/DNDF/Default.aspx"
ChromePATH='F:\Python\chromedriver.exe'
buttonsID='ctl00_PlaceHolderMain_biwebQueryContent_DataPagerListViewArtikelHits'

def dndf_ondisk():
    return

def get_dndflinks(m):
    nd_dates=[]
    links=[]
    driver = webdriver.Chrome(ChromePATH)
    driver.get(URL)
    time.sleep(3)
    for j in range(2,m+1):
        for k in range(0,10):
            container = id_prefix+str(k)+id_suffix
            dndfdate = date_prefix+str(k)+date_suffix
            cell = driver.find_element_by_id(container)
            cell_date = driver.find_element_by_id(dndfdate)
            links.append(cell.get_attribute('href'))
            nd_dates.append(cell_date.text)
            print(cell_date.text)
        buttons = driver.find_element_by_id(buttonsID)
        if j>10 and j%10==1:
            elem=buttons.find_element_by_link_text('>')
        else:
            elem=buttons.find_element_by_link_text(str(j))
        elem.click()
        time.sleep(5)
    return nd_dates, links

def loadDNDF_withlinks(nd_dates,links):
    columns = ['date','link','size_1m','size_3m', 'rate_1m','rate_3m']
    dndf = pd.DataFrame(columns=columns)
    for i in range(0,len(links)):
        URL1=links[i]
        linkdate=nd_dates[i]
        page1 = requests.get(URL1)
        time.sleep(3)
        soup1 = BeautifulSoup(page1.content, 'html.parser')
        table1 = soup1.find('table', class_='MsoNormalTable')
        if table1 is not None:
            rows = table1.find_all('tr')
            data=[]
            for row in rows:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                data.append([ele for ele in cols if ele])  
            datarow={'date':linkdate,'link':URL1,'size_1m':data[1][2],'size_3m':data[2][2], 'rate_1m':data[1][1],'rate_3m':data[2][1]}
            print(datarow)
            dndf=dndf.append(datarow,ignore_index=True)    
    return dndf


def process_dndf(df,file):
    df.size_1m=df.size_1m.apply(tostr)
    df.size_3m=df.size_3m.apply(tostr)
    df.rate_1m=df.rate_1m.apply(tostr)
    df.rate_3m=df.rate_3m.apply(tostr)
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')
    df=df.groupby('date').agg(
        {
         'size_1m':sum,
         'size_3m':sum,
         'rate_1m':"mean",
         'rate_3m':"mean"
         }
        )
    df.to_csv(file)
    return df

def tostr(x):
    if isinstance(x, int) == False:
        x=x.replace('Rp.','')
        if x.replace('.','').isdigit():
            x=int(x.replace('.',''))
        else:
            x=0
    return x                

dndf=pd.read_csv("dndf1.csv",parse_dates=['date'],index_col=['date'])
size1m=dndf.resample('W').sum()
