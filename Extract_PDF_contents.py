#!/Users/privateprivate/envs/bin/python

import PyPDF2, os, time
import pandas as pd
'''
Standalone FWBON GBIF species mapping script
@ Author: Marc Pienaar (m.pienaar@saeon.nrf.ac.za, marc.pienaar@gmail.com)
'''
#record the start time
start_time = time.time()
# Creating a pdf file object from a FADA list
pdfFileObj = open('/Users/privateprivate/Downloads/Macrophytes.pdf','rb')
# Creating a pdf reader object
pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
# Getting number of pages in pdf file
pages = pdfReader.numPages

output=[]
# Loop for reading all the Pages
for i in range(pages):
    # Creating a page object
    pageObj = pdfReader.getPage(i)
    # Extracting text from page
    # And splitting it into chunks of lines
    text = pageObj.extractText().split("  ")
#   text=text.split('\n')
    # Finally the lines are stored into list
    # For iterating over list a loop is used
    for j in range(len(text)):
        a=(text[j])
        output.append(a)
#       print(text[i],end="\n\n")
    # For Seprating the Pages
#   print()
# closing the pdf file object
pdfFileObj.close()

#print(output)
res = [ele for ele in output if ele != []]
res = [ele for ele in res if ele != '\n']
while('' in res):
    res.remove('')
df=pd.DataFrame(res)
print(df)
#save to file
outputfile='/Users/privateprivate/SAEON/GBIF/CODE/GBIF_FWBON/DATA/temp/temp.xlsx'
df.to_excel(outputfile,index=False)
print("total time took --- %s seconds ---" % (time.time() - start_time))
#for i in res:
#   print(i)