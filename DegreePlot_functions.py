#!/Users/privateprivate/envs/bin/python

from shapely.geometry import Polygon
import geopandas as gpd
from pygbif import occurrences as occ
from pygbif import species
from joblib import Parallel, delayed
import pandas as pd
import matplotlib.path as mpath
import matplotlib.patches as mpatches
import shapefile
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as pgon
import numpy as np
import matplotlib.pyplot as plt
from numpy import log10
import time
import matplotlib.colors as colors
import matplotlib.cm as mpl_cm

def process_species(x,M,geom, lenn):
    output=[]
    for i in range(0, len(M), 500):
        out = occ.search(speciesKey=M[i:i+500], geometry=geom,occurrenceStatus='PRESENT',facet='speciesKey',facetLimit=1000000,limit=0)
        for j in range(0,len(out['facets'][0]['counts'])):
            output.append(out['facets'][0]['counts'][j]['name'])
    print(x, "of", lenn, (x/lenn)*100)
    return output

def process_all(x,M,geom, lenn):
    output1=[]
    output2=[]
    output3=[]
    output4=[]
    count2=0
    for i in range(0, len(M), 500):
        out = occ.search(speciesKey=M[i:i+500],facet='speciesKey',facetLimit=1000000, geometry=geom,occurrenceStatus='PRESENT',limit=0)
        count2=count2+out['count']
        for j in range(0,len(out['facets'][0]['counts'])):
            output1.append(out['facets'][0]['counts'][j]['name'])
    count1=len(set(list(output1)))
    output2.append(count1)    
    output3.append(count2)
    output4.append(output2)
    output4.append(output3)
    print(x, "of", lenn, (x/lenn)*100)
    return output4

def plot(geoframe, colval, lengendlabel, titleval, outputfile):
    sf = shapefile.Reader("/Users/privateprivate/Downloads/ne_50m_admin_0_countries/ne_50m_admin_0_countries.shp")
    recs    = sf.records()
    shapes  = sf.shapes()
    Nshp    = len(shapes)
    cns     = []
    for nshp in range(Nshp):
        cns.append(recs[nshp][1])
    cns = np.array(cns)
    plt.rcParams.update({'font.size': 12})
    fig, ax = plt.subplots(1, 1, figsize = (14, 6))
    brewer_cmap = mpl_cm.get_cmap('turbo')
    geoframe.plot(column = colval, 
        ax = ax, 
        legend = True, 	
        cmap = brewer_cmap,
        #	norm=norm,
        legend_kwds={'label': lengendlabel, 
            'shrink': 0.5})
    for nshp in range(Nshp):
        ptchs   = []
        pts     = np.array(shapes[nshp].points)
        prt     = shapes[nshp].parts
        par     = list(prt) + [pts.shape[0]]
        for pij in range(len(prt)):
            ptchs.append(pgon(pts[par[pij]:par[pij+1]]))
            ax.add_collection(PatchCollection(ptchs,facecolors="None",edgecolor='k', linewidths=1))
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title(titleval,fontdict = {'fontsize': 'x-large'})
    resolution_value = 300
    plt.tight_layout()
    plt.savefig(outputfile, format="png", dpi=resolution_value)
    plt.close()
    
def get_poly_list(size):
    long_range = list(range(-180, 180,size))
    lat_range = list(range(-90, 90,size))
    poly_list = []
    for x in long_range:
        for y in lat_range:
            new_poly = Polygon([(x, y), 
                (x + size, y), 
                (x + size, y + size), 
                (x, y + size)])
            poly_list.append(new_poly)
    return poly_list
    
input='/Users/privateprivate/SAEON/GBIF/CODE/GBIF_FWBON/DATA/temp/temp2.xlsx'
df = pd.read_excel(input)
key_in_dict=df['speciesKey'].dropna()
key_in_dict = list(set(filter(None, key_in_dict)))
#make sure the search keys are of dtype = int
key_in_dict=list(map(int, key_in_dict))
print(len(key_in_dict))


#key_in_dict=list(set(FADA.FADA_fish2))
namme='FADA_Vertebrates_mammals2'
start_time = time.time()

grid_df_1d = gpd.GeoDataFrame(geometry = get_poly_list(60), crs = "EPSG:4326")
grid_df_1d2 = gpd.GeoDataFrame(geometry =  get_poly_list(5), crs = "EPSG:4326")


results=Parallel(n_jobs=12)(delayed(func.process_species)(i,key_in_dict,grid_df_1d.geometry[i],len(grid_df_1d)) for i in range(len(grid_df_1d)))
output=[]
for j in range(0,len(results)): 
    for i in results[j]:
        output.append(i)
        
total=len(set(list(output)))

results2=Parallel(n_jobs=12)(delayed(func.process_all)(i,key_in_dict,grid_df_1d2.geometry[i],len(grid_df_1d2)) for i in range(len(grid_df_1d2)))


output2=[]#species
output3=[]#occurence
for j in range(0,len(results2)): 
#	print(results2[j][0])
    for i in results2[j][0]:
        output2.append(i)
    for i in results2[j][1]:
        output3.append(i)
        
        
#print(output3)
total2=sum(output3)
gdf = grid_df_1d2#[0:20]
gdf['species'] = output2
gdf['occurrence'] = output3



t=[]
t1=[]

#
for i in output2:#species
    if i >0:
        t.append(np.log(i))#
    else:
        t.append(i)#
#		
for i in output3:#occurence
    if i >0:
        t1.append(np.log(i))#
    else:
        t1.append(i)#
#
gdf['species_logval'] = t
gdf['occurrence_logval'] = t1



func.plot(gdf, 'species', "Species $(n)$", namme +"[species (N =" + str(int(total)) + ")]", '/Users/privateprivate/SAEON/GBIF/survey_maps/' + namme + '_five_degrreee_species.png')
func.plot(gdf, 'species_logval', "Species $\log (n)$",  namme + "[Species (N =" + str(int(total)) + ")]", '/Users/privateprivate/SAEON/GBIF/survey_maps/' + namme + '_five_degrreee_species_log.png')
#
func.plot(gdf, 'occurrence', "Occurrence counts $(n)$",namme + " occurrence counts (N =" + str(int(total2)) + ")", '/Users/privateprivate/SAEON/GBIF/survey_maps/' + namme + '_five_degrreee_occurence.png')
func.plot(gdf, 'occurrence_logval', "Occurrence counts $\log (n)$",namme +"occurrence counts (N =" + str(int(total2)) + ")", '/Users/privateprivate/SAEON/GBIF/survey_maps/' + namme + '_five_degrreee_occurence_log.png')
#

print("total time took --- %s seconds ---" % (time.time() - start_time))
