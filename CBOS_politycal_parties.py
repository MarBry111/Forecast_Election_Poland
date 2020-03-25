import urllib
import requests
import re 
import os

base_url = 'https://www.cbos.pl/PL/szukaj/szukaj.php?poczatek=1&na_stronie=311'
page = requests.get(base_url)
page.encoding = 'utf-8'

#<a href="open_file.php?url=1996/K_160_96.PDF&amp;tytul=Preferencje+wyborcze+w+pa;378;dzierniku+‘96" target="_blank" class="dymek"><img src="../grafika/acrobat.jpg" alt="Pobierz pdf"><span>komunikat<br>bez tabel zróżnicowań<br>socjo-demograficznych</span></a>
pliki = re.findall(r'<a href="(.*)" target="_blank" class="dymek"><img src="../grafika/acrobat.jpg" ', page.text)

print(pliki)
#print(page.text)

#urllib.url_retrive("http://example.com/helo.pdf","c://home")