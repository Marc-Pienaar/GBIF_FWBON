#!/Users/privateprivate/envs/bin/python

#import some packages
from pygbif import occurrences as occ
from pygbif import species
import pandas as pd
from joblib import Parallel, delayed
import os, time

'''
Standalone FWBON GBIF species matching script
@ Author: Marc Pienaar (m.pienaar@saeon.nrf.ac.za, marc.pienaar@gmail.com)
'''
#record the start time
start_time = time.time()

#A small parallel processing script to process the species names
def process(i,speciesList):
	'''
	@ Author Marc Pienaar (m.pienaar@saeon.nrf.ac.za, marc.pienaar@gmail.com)
	
	A small parallel processing script to process the species names in GBIF

	Parameters
	----------
	i : int
		the index of a species name in a species list.

	speciesList : Array[]
		A checklist of species names

	Returns
	-------
	output :  Array[]
		Various values from GBIF attributes as an Array
	'''
	#some attribute vlaues we want to return	
	attributes=['usageKey', 'acceptedUsageKey','scientificName', 'canonicalName', 'rank', 'status', 'confidence', 'matchType', 'kingdom', 'phylum','class', 'order', 'family', 'genus', 'species', 'kingdomKey', 'phylumKey', 'classKey', 'orderKey', 'familyKey', 'genusKey', 'speciesKey']	
	output=[] #The main output array
	x=speciesList[i] #The species name	
	vals={} #A temporary dictionaty
	temp=[] #A temporary array
	speciesKeyCount='' #A vriable to return the number of occurences for this species if it exists	
	#pre-popoulate some values
	vals['input']=x
	vals['count_usageKey']=''
	vals['count_speciesKey']=''
	try:
		x=x.rstrip();x=x.lstrip()
		key = species.name_backbone(x)
		out = occ.search(taxonKey = key['usageKey'], limit=0)
		try:
			s = occ.search(speciesKey = key['speciesKey'], limit=0)
			speciesKeyCount =s['count']
		except:
			speciesKeyCount=''
		vals['count_usageKey']=out['count']
		vals['count_speciesKey']=speciesKeyCount
		for a in attributes:
			if a in key:
				vals[a] = key[a]
			else:
				vals[a] = ''
		for v in vals.keys():
			temp.append(vals[v])
	except:
		for a in attributes:
			vals[a] = ''
		for v in vals.keys():
			temp.append(vals[v])
	output.append(temp)	
	print(i)
	return(output)

#define an input and output
input=os.getcwd() + os.sep + 'DATA' + os.sep + 'Checklists' + os.sep + 'FADA_Vertebrates_Mammals.xlsx'
output=os.getcwd() + os.sep + 'DATA' + os.sep + 'GBIF_processed_Checklists' + os.sep + 'GBIF_FADA_Vertebrates_Mammals.xlsx'
input_colname='scientificName'
#read the input into a pandas dataFrame
df = pd.read_excel(input)
#get a list from the dataframe and remove empty values
speciesList=df[input_colname].dropna()
speciesList = list(filter(None, speciesList))
LENGTH=len(speciesList)	
#re use the df variable to create an empty dataFrame
df = pd.DataFrame({'input':[],'count_usageKey':[],'count_speciesKey':[],'usageKey':[],'acceptedUsageKey':[], 'scientificName':[], 'canonicalName':[], 'rank':[], 'status':[], 'confidence':[], 'matchType':[], 'kingdom':[], 'phylum':[], 'class':[], 'order':[], 'family':[], 'genus':[], 'species':[], 'kingdomKey':[], 'phylumKey':[], 'classKey':[], 'orderKey':[], 'familyKey':[], 'genusKey':[], 'speciesKey':[]})
#run the function usaing 12 cores
results=Parallel(n_jobs=12)(delayed(process)(i,speciesList) for i in range(LENGTH))
#populate the dataframe
for j in range(0,len(results)):
	df.loc[j]=results[j][0]	
#save to file
df.to_excel(output,index=False)
print("total time took --- %s seconds ---" % (time.time() - start_time))