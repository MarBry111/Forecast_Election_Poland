import os
import pandas as pd

def connect_one_df(year,level):
	folder = '../wyniki_wyborow/' + year + '/'

	dfTOTAL = None

	okregi = os.listdir(folder)
	def add_name_of_district(fol =None, ok = None ,d = 'O_'):
		df = None
		for o in ok:
			if not os.path.isdir(fol+o):
				#to add only powiat cities
				if True:#(d == 'P_' and not o[0].islower()) or d != 'P_':
					df = pd.read_csv(fol+o, index_col = 0, usecols = ['Komitet','Glosy'])
					df.columns =  [ d+o.split('.')[0]]
					df = df.transpose()
		return df

	dfTOTAL = add_name_of_district(fol =folder, ok = okregi ,d = '')
	for o in okregi:
		if os.path.isdir(folder+o):
			folder_1 = folder+o+'/'
			powiaty = os.listdir(folder_1)
			#dfTOTAL = pd.concat([dfTOTAL, add_name_of_district(fol =folder_1, ok = powiaty ,d = 'O_')])
			if('w'==level):
				dfTOTAL = pd.concat([dfTOTAL, add_name_of_district(fol =folder_1, ok = powiaty ,d = '')])
			else:
				for p in powiaty:
					if os.path.isdir(folder_1+p):
						folder_2 = folder_1+p+'/'
						gminy = os.listdir(folder_2)
						
						if('p'==level):
							dfTOTAL = pd.concat([dfTOTAL, add_name_of_district(fol =folder_2, ok = gminy ,d = '')])		
						else:
							for g in gminy:
								if os.path.isdir(folder_2+g):
									folder_3 = folder_2+g+'/'
									gminy = os.listdir(folder_3)
									dfTOTAL = pd.concat([dfTOTAL, add_name_of_district(fol =folder_3, ok = gminy ,d = '')])

	dfTOTAL.to_csv(year+'_'+level+'.csv', encoding='utf-8')

years = ['2001','2005', '2007', '2011']

for y in years:
	connect_one_df(y, 'p')