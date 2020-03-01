import tabula
import pandas as pd
import requests 
import json
from pandas.io.json import json_normalize
import numpy as np
import time
#BANK DANYCH LOKALNYCH

# api-endpoint 
URL = "https://bdl.stat.gov.pl"

clinets = ['56d00aa0-b498-449b-2f52-08d7649f3c05',
           'f0e9a93d-5874-4d20-2f50-08d7649f3c05',
           'a1e145a2-e602-491c-2f51-08d7649f3c05',
           '95e1e284-3c48-4d3e-2f4f-08d7649f3c05']


def get_request(URL, flag=False):
    # location given here 
    location = 'HTTP/1.1'

    # defining a params dict for the parameters to be sent to the API 
    PARAMS = {'address':location, 
              'Host':'hostname', 
              'X-ClientId': clinets[1],
              'page-size':100} 

    # sending get request and saving the response as response object 
    if flag:
        r = requests.get(url = URL)
    else:
        r = requests.get(url = URL, params = PARAMS) 

    # extracting data in json format 
    data = r.json()
    return data

def get_data(id_v, name_v):
    #v = 'P2137'
    #page_v = '/api/v1/Variables?subject-id='+v
    #data_v = get_request(URL+page_v)
    #id_v = str(data_v['results'][0]['id'])
    
    df = pd.DataFrame({'jednostka' : [],
                        '2001': [], 
                        '2002': [], '2003': [], '2004': [], '2005': [], '2006': [], '2007': [], 
                        '2008': [], '2009': [], '2010': [], '2011': [], '2012': [], '2012': [], 
                        '2013': [], '2014': [], '2015': [], '2016': [], '2017': [], '2018': [] })
    #id_v = '155055'
    #name_v = 'emeryci_i_rencisci'
    variable1 = '/api/v1/data/by-variable/'+id_v+'?lang=pl&format=json'
    data_j = get_request(URL+variable1)

    pages = data_j['totalRecords']//100+1
    url_v = None
    if pages > 1:
        url_v = data_j['links']['first'][:-1]

    for p in range(pages):
        if url_v:
            url_new = url_v+str(p)
            data_j = get_request(url_new)
        for dj in data_j['results']:
            years = dj['values']
            val = [dj['name']]
            if dj['name'].find('REGION') != -1:
                pass
            else:
                col = ['jednostka']
                for y in years:
                    val.append(y['val'])
                    col.append(y['year'])
                df = df.append( pd.DataFrame([val], columns=col), ignore_index=True, sort=False)

    df.to_csv('dane/'+name_v+'.csv',index=False)
    time.sleep(5)

# https://bdl.stat.gov.pl/BDL/metadane/cechy/2625
features = [['155055', 'emeryci_i_rencisci'], #przeciętna liczba emerytów i rencistów ogółem (NUTS-2)
            ['33507', 'bezrobocie_zarejsestrowane'], #Bezrobotni zarejestrowani ogółem (P)
            ['10514', 'bezrobocie_zarejsestrowane_gminy'], #Bezrobotni zarejestrowani ogółem (G, 2003-)
            ['58', 'malzenstwa_zawarte'], # (G)
            ['60559', 'ludnosc_na_1km2'], #(G, 2002 -)
            ['76037', 'dochody_gminy'], # (G)
            ['76973','dochody_na_mieszkanca'], #(G, 2002-)
            ['479300', 'wyksztalcenie_wyzsze'], #(NUTS 2)   Odsetek ludności w wieku 15-64 lata z wykształceniem wyższym wg płci i miejsca zamieszkania - różnica w stosunku do średniej krajowej (p.proc.) 
            ['478931', 'wyksztalcenie_gim_pod_nizsze'], #(NUTS 2)   Odsetek ludności w wieku 15-64 lata z wykształceniem wyższym wg płci i miejsca zamieszkania - różnica w stosunku do średniej krajowej (p.proc.) 
            ['478929', 'wyksztalcenie_srednie' ], #(NUTS 2)  średnie (łącznie ze średnim zawodowym i ogólnokształcącym); Odsetek ludności w wieku 15-64 lata z wykształceniem wyższym wg płci i miejsca zamieszkania - różnica w stosunku do średniej krajowej (p.proc.)            
            ['35039', 'rozwody_powiat'], # (P)
            ['60567', 'udzial_wiek_przedprodukcyjny'], # (G, 2002 -) Udział ludności wg ekonomicznych grup wieku w % ludności ogółem, przedprodukcyjny
            ['60566', 'udzial_wiek_produkcyjny'], # (G, 2002 -) produkcyjny
            ['60565', 'udzial_wiek_poprodukcyjny'], # (G, 2002 -) postprodukcyjny
            ['216969', 'praca_najemna'], # (NUTS 2) Przeciętny miesięczny dochód rozporządzalny na 1 osobę"
            ['216971', 'praca_wlasny_rachunek'], # (NUTS 2) 
            ['519999', 'socjal_500plus'], # (NUTS 2, 2016 -) 
            ['216972', 'socjal'], # (NUTS 2)
            ['458417', 'dochody_brutto_na_mieszkanca'], # (NUTS 2) dochody do dyspozycji brutto na 1 mieszkańca
            ['458421', 'PKB_na_1_mieszkanca'],  # (NUTS 2)
            ['58559', 'przestepstwa_ogolem'] # (NUTS 3, powiaty, 2002 -)
            ]

for f in [features[-1]]:
    print(f)
    get_data(f[0], f[1])
