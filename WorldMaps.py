#!/Users/privateprivate/envs/bin/python
#import some packages
from pygbif import occurrences as occ
from pygbif import species
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from joblib import Parallel, delayed
import os, time
import numpy as np
'''
Standalone FWBON GBIF species mapping script
@ Author: Marc Pienaar (m.pienaar@saeon.nrf.ac.za, marc.pienaar@gmail.com)
'''
#record the start time
start_time = time.time()
#A small parallel processing script to process the number of species per country in GBIF
def process_species(x,speciesList,country):
    '''
    @ Author Marc Pienaar (m.pienaar@saeon.nrf.ac.za, marc.pienaar@gmail.com)
    
    A small parallel processing script to process the number of species per country in GBIF

    Parameters
    ----------
    x : int
        the index of a country code in a list.

    speciesList : Array[]
        speciesKeys to search for

    country : Array[]
        An array of counry codes

    Returns
    -------
    An Array[[country code][species counts][occurrence counts]] 
    '''
    count=[]
    count_species=[]
    
    #here we call the occurrrnce search multiple times in batches incase the species list is very large!
    for i in range(0, len(speciesList), 500):
        out = occ.search(speciesKey=speciesList[i:i+500],country=country[x], limit=0, facet=['speciesKey'],facetLimit=1000000)
        for j in range(0,len(out['facets'][0]['counts'])):
            count_species.append(out['facets'][0]['counts'][j]['name'])
        count.append(out['count'])
    count_species=list(set(count_species))	
    print(country[x])
    return [country[x], count_species, sum(count)]

def Plot(shape, name, col, legend_lable,count,outputfile):
    '''
    @ Author Marc Pienaar (m.pienaar@saeon.nrf.ac.za, marc.pienaar@gmail.com)
    
    A small plotting function

    Parameters
    ----------
    shape : geodataFrame 
        to use for plotting

    col : String
        the name of the column from the.

    name : String
        the tile name for the plot.
    
    legend_lable : String
        the label for the legend

    count : int
        the count vale to display fo N

    outputfile : String
        the path to write the map to

    Returns
    -------
    A map saved to the outputfile
    '''
    plt.rcParams.update({'font.size': 12})
    fig, ax = plt.subplots(1, 1, figsize = (14, 6))
    shape.plot(column = col, 
        ax = ax, 
        legend = True, 	
        cmap = 'turbo',
        legend_kwds={'label': legend_lable, 
            'shrink': 0.5})
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title(name + ' (N='+str(count)+')')
    resolution_value = 300
    plt.tight_layout()
    plt.savefig(outputfile, format="png", dpi=resolution_value)
    plt.close()

#input and outputs
input=os.getcwd() + os.sep + 'DATA' + os.sep + 'GBIF_processed_Checklists' + os.sep + 'GBIF_FADA_Vertebrates_Mammals.xlsx'
outpath = os.getcwd() + os.sep + 'DATA' + os.sep + 'Maps'+ os.sep + 'GBIF_FADA_Macrophytes' + os.sep
outputfile1=outpath+'GBIF_FADA_Vertebrates_Mammals_occurrence_world.png'
outputfile2=outpath  + 'GBIF_FADA_Vertebrates_Mammals_occurrence_world_log.png'
outputfile3=outpath+'GBIF_FADA_Vertebrates_Mammals_species_world_log.png'
outputfile4=outpath+'GBIF_FADA_Vertebrates_Mammals_counts.xlsx'


#get a list of country codes fom GBIF
country=[]
out=occ.search(facet='country',facetLimit=1000000,limit=0)
for j in range(0,len(out['facets'][0]['counts'])):
    country.append(out['facets'][0]['counts'][j]['name'])

#get the speciesKeys to use for searching
df = pd.read_excel(input)
speciesList=df['speciesKey'].dropna()
speciesList = list(set(filter(None, speciesList)))
#make sure the search keys are of dtype = int
speciesList=list(map(int, speciesList))
print(len(speciesList))
#get a high definition map with the same country codes as GBIF - here an external map already formated -- too big to load to github
world_map = gpd.read_file('/Users/privateprivate/SAEON/GBIF/data/ipbes_regions_subregions_shape_1.1/IPBES_Regions_Subregions2.shp')
country_shapes = world_map[['geometry', 'ISO_3','ISO_2']]
#reuse thee df variable o create a blank dataFrame
df = pd.DataFrame()
#run the function usaing 12 cores for the various countries
results=Parallel(n_jobs=12)(delayed(process_species)(i,speciesList,country) for i in range(len(country)))
#return the results into seperate arrays
output1=[]
output2=[]
output3=[]
for j in range(0,len(results)): 
    output1.append(results[j][0])
    output2.append(results[j][1])
    output3.append(results[j][2])    
    
output4=[]
output5=[]
for j in range(0,len(output2)):
    output4.append(len(output2[j]))
    for i in output2[j]:
        output5.append(i)
#prepopulate the df
df['ISO_2']=output1
df['species']=output4
df['log_count']=output3
df['counts']=output3
#sort by country
df=df.sort_values(by=['ISO_2'])
countt=0
logcount=[]
for i in df['log_count']:
    if i >0:
        logcount.append(np.log(i))
    else:
        logcount.append(i)
df['log_count']=logcount
#Total unique species count
count_species=len(set(output5))
#Total ocurence counts
countt=df['counts'].sum()
#combine the geodataframe with the dataframe
country_shapes = country_shapes.merge(df, on='ISO_2')
#create three output plots
Plot(country_shapes, 'FADA_Vertebrates_Mammals occurrence counts','counts', 'Occurrence counts (n)',countt, outputfile1)
Plot(country_shapes, 'FADA_Vertebrates_Mammals occurrence counts','log_count', 'Occurrence counts log(n)',countt,outputfile2)
Plot(country_shapes, 'FADA_Vertebrates_Mammals species counts','species', 'Species counts (n)',count_species,outputfile3)
#save to file
df.to_excel(outputfile4,index=False)
print("total time took --- %s seconds ---" % (time.time() - start_time))