import pandas as pd
import numpy as np
import os

def df_mapping_regions_to_wojew():
    """
    Function for loading maaping of regions into districts
    """
    df_jednostki = pd.read_csv('dane/_jednostki.csv', sep=';')
    df_jednostki.okręgi = [ o.replace(' ', '_') for o in df_jednostki.okręgi ]
    df_jednostki.województwo = [ w.upper() for w in df_jednostki.województwo ]

    df_jednostki.loc[df_jednostki['województwo']=='WARSZAWA','województwo'] = 'MAZOWIECKIE'

    return df_jednostki

def clustering_2001(path_in="wyniki_wyborow/2001_W.csv", path_out="wyniki_wyborow/2001_O_simplified.csv"):
    """
    Function for clustering voting data from 2001

    Input:
    path_in : string
        string with path to voting data from 2001 
    path_out : string
        string with path where to save mped values 

    Return:
        : DataFrame 
        dataframe with mapped data
    """
    # read csv file
    wyniki_2001 = pd.read_csv(path)
    # rename columns
    col = wyniki_2001.columns.values.tolist()
    col[0] = 'jednostka'
    wyniki_2001.columns = col
    # deleting first part indicating that we are dealign with region in jednostka column
    wyniki_2001['jednostka'] = [ w.replace('O_', '') for w in wyniki_2001['jednostka'] ]
    # filling NaN va;ues with 0 (no data == 0 votes)
    wyniki_2001 = wyniki_2001.fillna(0)
    # value wich will be mapped into given "stronnictwo"
    col_b = ['LPR','PSL','PiS','Samoobrona']
    col_r = ['PO']
    col_g = [c for c in wyniki_2001.columns.values.tolist() if c not in col_r+col_b+['jednostka','nr','okręgi','powiaty','SLD']]
    # mapping values into specific party
    wyniki_2001['Blue'] = wyniki_2001.loc[:, col_b].sum(axis = 1, skipna = True) 
    wyniki_2001['Red'] = wyniki_2001.loc[:, col_r].sum(axis = 1, skipna = True) 
    wyniki_2001['Gray'] = wyniki_2001.loc[:, col_g].sum(axis = 1, skipna = True) 
    # selecting interesting columns, renaming them and uppering
    wyn_01 = wyn_01[['jednostka','Blue','Red','Gray']].iloc[1:,:]
    wyn_01.columns = ['województwo','Blue','Red','Gray']
    wyn_01['województwo'] = [w.upper() for w in wyn_01['województwo'] ]
    # setting districts as index and sorting by it values
    wyn_01 = wyn_01.set_index('województwo').sort_index()
    # saving to file
    wyn_01.to_csv(path_out)

    return wyn_01


def clustering_2005_07_11(year=2005, path_in="wyniki_wyborow/2005_O.csv", path_out="wyniki_wyborow/2005_O_simplified.csv"):
    """
    Function for clustering voting data from 2005/07/11/17

    Input:
    year : int
    	yer which will be clustered
    path_in : string
        string with path to voting data from 2005/07/11/15
    path_out : string
        string with path where to save mped values 

    Return:
        : DataFrame 
        dataframe with mapped data
    """
    # read csv file
    wyniki = pd.read_csv(path_in)
    # rename columns
    col = wyniki.columns.values.tolist()
    col[0] = 'jednostka'
    wyniki.columns = col
    # deleting first part indicating that we are dealign with region in jednostka column
    wyniki['jednostka'] = [ w.replace('O_', '') for w in wyniki['jednostka'] ]
    # filling NaN values with 0 (no data == 0 votes)
    wyniki = wyniki.fillna(0)
    # load mapping file and join with dataframe with voting data
    df_jednostki = df_mapping_regions_to_wojew()
    wyn = wyniki.merge(df_jednostki,how='outer',left_on=['jednostka'],right_on=['okręgi'])
    # value wich will be mapped into given "stronnictwo"
    if(year==2005):
	    col_b = ['LPR','PSL','PiS','Samoobrona']
	    col_r = ['SLD','PO']
	elif(year==2007):
		col_b = ['PSL','PiS']
		col_r = ['SLD','PO']
	elif(year==2011):
		col_b = ['PiS']
		col_r = ['SLD','PO','Ruch Palikota/Twój Ruch','PSL']
	elif(year==2015):
		col_b = ['PiS','PSL','Kukiz']#,'KORWIN/ Wolnośc/ KONFEDERACJA']
		col_r = ['SLD','PO','Nowoczesna.pl']#,'Wiosna','Razem']
	col_g = [c for c in wyn.columns.values.tolist() if c not in col_r+col_b+['jednostka','nr','okręgi','powiaty']]
   # mapping values into specific party
    wyn['Blue'] = wyn.loc[:, col_b].sum(axis = 1, skipna = True) 
    wyn['Red'] = wyn.loc[:, col_r].sum(axis = 1, skipna = True) 
    wyn['Gray'] = wyn.loc[:, col_g].sum(axis = 1, skipna = True)
    # grouping by district
    wyn = wyn.groupby('województwo').agg(np.sum) 
    # selecting interesting columns and sorting by index
    wyn = wyn[['Blue','Red','Gray']].sort_index()
    # saving to file
    wyn.to_csv(path_out)

    return wyn

def clustering_2019(path_in="wyniki_wyborow/2019_O.csv", path_out="wyniki_wyborow/2005_O_simplified.csv"):
    """
    Function for clustering voting data from 2019

    Input:
    path_in : string
        string with path to voting data from 2019
    path_out : string
        string with path where to save mped values 

    Return:
        : DataFrame 
        dataframe with mapped data
    """
    # read csv file
    wyniki = pd.read_csv(path_in)
    # rename columns
	col = wyniki.columns.values.tolist()
	col[0] = 'województwo'
	wyniki.columns = col
    # filling NaN values with 0 (no data == 0 votes)
	wyniki = wyniki.fillna(0)
	# choose only interesting rows
	wyn_19 = wyniki.iloc[2:,:]
	# district names to upper
	wyn_19['województwo'] = [x.upper() for x in wyn_19['województwo']]
	# change type of each column despite first
	for c in wyn_19.columns[1:]:
	    wyn_19[c] = wyn_19[c].astype(float)
    # value wich will be mapped into given "stronnictwo"
	col_b = ['PiS','KORWIN/ Wolnośc/ KONFEDERACJA','PSL']
	col_r = ['SLD','PO']#,'Wiosna','Razem']
	col_g = [c for c in wyn_19.columns.values.tolist() if c not in col_r+col_b+['województwo','nr','okręgi','powiaty']]
	# mapping values into specific party
	wyn_19['Blue'] = wyn_19.loc[:, col_b].sum(axis = 1, skipna = True) 
	wyn_19['Red'] = wyn_19.loc[:, col_r].sum(axis = 1, skipna = True) 
	wyn_19['Gray'] = wyn_19.loc[:, col_g].sum(axis = 1, skipna = True) 
	# grouping by district
	wyn_19 = wyn_19.groupby('województwo').agg(np.sum)
	# selecting interesting columns and sorting by index
	wyn_19 = wyn_19[['Blue','Red','Gray']].sort_index()
	# saving to file
	wyn_19.to_csv(path_out)

	return wyn_19