#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import concurrent.futures
import requests
import re 
import os
import pandas as pd

#2011
base_url = 'https://wybory2011.pkw.gov.pl/wyn/'

main_page_url = base_url+'pl/000000.html'
page = requests.get(main_page_url)
page.encoding = 'utf-8'

path_0 = '/home/marek/Dokumenty/MGR_CLEAN/wyniki_wyborow/2011/'
#<a href="../../rfl/000000/pl/725c4668ea3ae43c83d5d5b737d3e93d.html">Komitet Wyborczy Prawo i Sprawiedliwość</a></td><td class="all_right">4 295 016</td>
wojew = re.findall(r'<a href=".*.html">([\w ]+)</a></td><td class="all_right">([0-9\s ]*)', page.text)
df_0 = pd.DataFrame(wojew, columns =['Komitet', 'Glosy']) 
df_0.Glosy = df_0.Glosy.str.replace('\s', '')

df_0.to_csv(path_0+'Polska.csv')

print('Polska')

#geting the subpages and names of districts

#<td class="all_left"><a href="../020000/pl/okr-1.html?tab=2">Legnica</a></td>
jednostki = re.findall(r'<td class="all_left"><a href="../(.*okr-\w*.html)\?tab=2">([\w\- ]+)</a></td>', page.text)
jednostki = list(set(jednostki))

def process_district(jednostka):
    path = path_0+jednostka[1].replace(" ", "_")
    try: os.mkdir(path)
    except: pass
    
    subpage_url = base_url+jednostka[0]
    subpage = requests.get(subpage_url)
    subpage.encoding = 'utf-8'

    #<a  class='top-bg' href="/rfl/pl/d46c53b10b6319f90c12824972b3e31e.html" title="Wyniki głosowania na listę">Komitet Wyborczy Prawica</a> - Zarejestrowana</td>
    komitety = re.findall(r'''<a  class='top-bg' href="/rfl/pl/.*.html" title="Wyniki głosowania na listę">([\w\- ]+)</a>''', subpage.text)
    #<a class="glob" href="/rfl/pl/7de3c03a29db80585deab9bfac6b66c2.html" title="Wyniki głosowania na listę"><strong>158 938</strong></a></td>
    glosy = re.findall(r'''<strong>([0-9]*.[^%]+[0-9]*)</strong></a>''', subpage.text)
    glosy = [g.replace("\xa0", '') for g in glosy]
    df = pd.DataFrame({'Komitet' : komitety,
                        'Glosy' : glosy}) 
    
    woj = jednostka[1].replace(" ", "_")

    df.to_csv(path+'/'+woj+'.csv')
    
    #<td class="all_left"><a href="180700.html">krośnieński, pow.</a></td>
    powiaty = re.findall(r'<td class="all_left"><a href="(.*.html)">([^0-9][\w\- ]+)([\w\,\.\- ]*?)</a></td>', subpage.text)
    
    for powiat in powiaty:
        subpath = path+'/'+powiat[1]
        try: os.mkdir(subpath)
        except: pass
        
        sub_base_url = subpage_url.split("okr")[0]

        subsubpage_url = sub_base_url+powiat[0]
        subsubpage = requests.get(subsubpage_url)
        subsubpage.encoding = 'utf-8'

        komitety_2 = re.findall(r'''<a  class='top-bg' href="/rfl/pl/.*.html" title="Wyniki głosowania na listę">([\w\- ]+)</a>''', subpage.text)
        glosy_2 = re.findall(r'''<strong>([0-9]*.[^%]+[0-9]*)</strong></a>''', subpage.text)
        glosy_2 = [g.replace("\xa0", '') for g in glosy_2]
        df_2 = pd.DataFrame({'Komitet' : komitety,
                            'Glosy' : glosy}) 
        df_2.to_csv(subpath+'/'+powiat[1]+'.csv')
        

        #<a href="142003.html">Baboszewo, gm.</a>
        gminy = re.findall(r'<td class="all_left"><a href="(.*.html)">([^0-9][\w\- ]+)[\w\,\.\- ]*?</a></td>', subsubpage.text)
        
        if powiat[2] == ', pow.':
            for gmina in gminy:
                subsubpath = subpath+'/'+gmina[1]
                try: os.mkdir(subsubpath)
                except: pass
                
                subsubsubpage_url = sub_base_url+gmina[0]
                subsubsubpage = requests.get(subsubsubpage_url)
                subsubsubpage.encoding = 'utf-8'
                
                komitety_3 = re.findall(r'''<a  class='top-bg' href="/rfl/pl/.*.html" title="Wyniki głosowania na listę">([\w\- ]+)</a>''', subpage.text)
                glosy_3 = re.findall(r'''<strong>([0-9]*.[^%]+[0-9]*)</strong></a>''', subpage.text)
                glosy_3 = [g.replace("\xa0", '') for g in glosy_3]
                df_3 = pd.DataFrame({'Komitet' : komitety,
                                    'Glosy' : glosy}) 
                df_3.to_csv(subsubpath+'/'+gmina[1]+'.csv')
            
    print(jednostka[1])

with concurrent.futures.ProcessPoolExecutor() as executor:
    executor.map(process_district, jednostki)