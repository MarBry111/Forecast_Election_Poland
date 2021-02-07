import pandas as pd

# 2019
df_2019 = pd.read_csv('wyniki_wyborow/2019/wyniki_gl_na_listy_po_wojewodztwach_sejm_csv/wyniki_gl_na_listy_po_wojewodztwach_sejm.csv', delimiter =';').fillna(0)

col = df_2019.columns.values.tolist()
select_col=[ 'Województwo',
             'KOALICYJNY KOMITET WYBORCZY KOALICJA OBYWATELSKA PO .N IPL ZIELONI - ZPOW-601-6/19',
             'KOMITET WYBORCZY AKCJA ZAWIEDZIONYCH EMERYTÓW RENCISTÓW - ZPOW-601-21/19',
             'KOMITET WYBORCZY KONFEDERACJA WOLNOŚĆ I NIEPODLEGŁOŚĆ - ZPOW-601-5/19',
             'KOMITET WYBORCZY POLSKIE STRONNICTWO LUDOWE - ZPOW-601-19/19',
             'KOMITET WYBORCZY PRAWICA - ZPOW-601-20/19',
             'KOMITET WYBORCZY PRAWO I SPRAWIEDLIWOŚĆ - ZPOW-601-9/19',
             'KOMITET WYBORCZY SKUTECZNI PIOTRA LIROYA-MARCA - ZPOW-601-17/19',
             'KOMITET WYBORCZY SOJUSZ LEWICY DEMOKRATYCZNEJ - ZPOW-601-1/19',
             'KOMITET WYBORCZY WYBORCÓW KOALICJA BEZPARTYJNI I SAMORZĄDOWCY - ZPOW-601-10/19',
             'KOMITET WYBORCZY WYBORCÓW MNIEJSZOŚĆ NIEMIECKA - ZPOW-601-15/19']
to_group=['Województwo']

df_2019 = df_2019.loc[df_2019['Województwo'] != 0, :]

df_2019[select_col]
df_2019[select_col[1:]] = df_2019[select_col[1:]].apply(pd.to_numeric)

df_output = df_2019[select_col].groupby(to_group).agg(['sum'])
df_output.to_csv('wyniki_wyborow/2019_O.csv')

# 2015
list_excel = []
for i in range(41):
    excel_pd = pd.read_excel('wyniki_wyborow/2015/'+str(i+1).zfill(2)+'.xlsx').fillna(0)
    cols_new = [True if 'Razem ' in ec else False for ec in excel_pd.columns.values.tolist()]
    cols_new = excel_pd.columns[cols_new]
    excel_pd = excel_pd[cols_new]
    excel_pd = excel_pd.replace(to_replace="XXXXX",value =0) 
    excel_pd = excel_pd.apply(pd.to_numeric)
    excel_pd = excel_pd.sum()
    list_excel.append(excel_pd)

df_2015 = pd.concat(list_excel, axis=1).T.fillna(0)

df_2015['okrag'] = [i+1 for i in range(41)]
df_2015.to_csv('wyniki_wyborow/2015_O.csv')