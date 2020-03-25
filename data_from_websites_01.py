#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import concurrent.futures
import requests
import re 
import os
import pandas as pd
from bs4 import BeautifulSoup

#2001
base_url = 'https://wybory2001.pkw.gov.pl/'

main_page_url = base_url+'sjg2_k.html'
page = requests.get(main_page_url)
page.encoding = 'utf-8'


# here, we fetch the content from the url, using the requests library
page_content = BeautifulSoup(page.content, "html.parser")
#we use the html parser to parse the url content and store it in a variable.textContent = []
paragraphs = page_content.findAll('a')
for p in paragraphs:
	print(p)
# In my use case, I want to store the speech data I mentioned earlier.  so in this example, I loop through the paragraphs, and push them into an array so that I can manipulate and do fun stuff with the data.



path_0 = '/home/marek/Dokumenty/MGR_CLEAN/wyniki_wyborow/2001/'
#<a href="sjg2_k7k.html">Koalicyjny Komitet Wyborczy Sojusz Lewicy Demokratycznej - Unia Pracy</a>
#<td><a href="sjg2_k7k.html">Koalicyjny Komitet Wyborczy Sojusz Lewicy Demokratycznej - Unia Pracy</a></td><td align="right">5342519</td>
#<td><a href="sjg2_k6k.html">Komitet Wyborczy Unii Wolno�ci</a></td><td align="right">404074</td>
wojew = re.findall(r'<td><a href=".*.html">([\w\,\.\-\"\'\s ]+)</a>', page.text)


#</td><td align="right">([0-9]+)</td>
print(wojew)

df_0 = pd.DataFrame(wojew, columns =['Komitet', 'Glosy']) 
df_0.Glosy = df_0.Glosy.str.replace('\s', '')


if(False):
	df_0.to_csv(path_0+'Polska.csv')

	print('Polska')

	#geting the subpages and names of districts

	#<a href="020000/sjg2_w.html">dolnośląskie</a>
	jednostki = re.findall(r'<td class="all_left"><a href="../(.*okr-\w*.html)\?tab=2">([\w\,\.\- ]*?)</a></td>', page.text)
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