#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import concurrent.futures
import requests
import re 
import os
import pandas as pd

base_url = 'https://wybory2005.pkw.gov.pl/SJM/PL/WYN/W/'
main_page_url = base_url+'index.htm'
page = requests.get(main_page_url)
page.encoding = 'utf-8'

path_0 = '/home/marek/Dokumenty/MGR_CLEAN/wyniki_wyborow/2005/'

wojew = re.findall(r'<td class="col4al"><a href=".*.htm" class="link1">([\w\- ]+)</a></td><td class="col5ar">([0-9]*)</td>', page.text)
df_0 = pd.DataFrame(wojew, columns =['Komitet', 'Glosy']) 

df_0.to_csv(path_0+'Polska.csv')

print('Polska')

#geting the subpages and names of districts
jednostki = re.findall(r'<a href="([1-9][0-9]?.htm)" CLASS="link1">([\w\-\. ]+)</a>', page.text)


def process_district(jednostka):
    path = path_0+jednostka[1].replace(" ", "_")
    try: os.mkdir(path)
    except: pass
    
    subpage_url = base_url+jednostka[0]
    subpage = requests.get(subpage_url)
    subpage.encoding = 'utf-8'
    #<a href="../../WYN/W85/1.htm" class="link1">Komitet Wyborczy Ruch Patriotyczny</a>
    komitety = re.findall(r'<td class="col4al"><a href=".*.htm" class="link1">([\w\- ]+)</a></td><td class="col5ar">([0-9]*)</td>', subpage.text)
    df = pd.DataFrame(komitety, columns =['Komitet', 'Glosy']) 
    
    woj = jednostka[1].replace(" ", "_")

    df.to_csv(path+'/'+woj+'.csv')
    
    #td class="col5al"><a href="320100.htm" class="link1">białogardzki, pow.</a></td>
    powiaty = re.findall(r'<td class="col5al"><a href="(.*.htm)" class="link1">([\w\- ]+)[\w\,\.\- ]*?</a></td>', subpage.text)
    
    for powiat in powiaty:
        subpath = path+'/'+powiat[1]
        try: os.mkdir(subpath)
        except: pass
        
        subsubpage_url = base_url+powiat[0]
        subsubpage = requests.get(subsubpage_url)
        subsubpage.encoding = 'utf-8'
        
        komitety_2 = re.findall(r'<td class="col4al"><a href=".*.htm" class="link1">([\w\- ]+)</a></td><td class="col5ar">([0-9]*)</td>', subsubpage.text)
        df_2 = pd.DataFrame(komitety_2, columns =['Komitet', 'Glosy']) 
        df_2.to_csv(subpath+'/'+powiat[1]+'.csv')
        
        #<td class="col5al"><a href="020102.htm" class="link1">Bolesławiec, gm.</a></td>
        gminy = re.findall(r'<td class="col5al"><a href="([0-9]*.htm)" CLASS="link1">([\w\,\.\- ]+)*[\w\,\.\- ]+</a></td>', subsubpage.text)
        
        for gmina in gminy:
            subsubpath = subpath+'/'+gmina[1]
            try: os.mkdir(subsubpath)
            except: pass
            
            subsubsubpage_url = base_url+gmina[0]
            subsubsubpage = requests.get(subsubsubpage_url)
            subsubsubpage.encoding = 'utf-8'
            
            komitety_3 = re.findall(r'<td class="col4al"><a href=".*.htm" class="link1">([\w\- ]+)</a></td><td class="col5ar">([0-9]*)</td>', subsubsubpage.text)
            df_3 = pd.DataFrame(komitety_3, columns =['Komitet', 'Glosy']) 
            df_3.to_csv(subsubpath+'/'+gmina[1]+'.csv')
        
    print(jednostka[1])


with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(process_district, jednostki)

