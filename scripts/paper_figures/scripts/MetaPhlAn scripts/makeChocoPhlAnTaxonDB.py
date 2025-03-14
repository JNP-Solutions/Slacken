import pickle
import bz2
import pandas as pd

db = pickle.load(bz2.open('/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/MetaPhlAnData/mpa_vJun23_CHOCOPhlAnSGB_202403.pkl', 'r'))

taxList=list(db['taxonomy'].items())
formattedList=[(a,b,c) for a, (b,c) in taxList]

def selectLast(c):
    return c.split('|')[-2]

df=pd.DataFrame(data=formattedList)
df.columns=['name','lineage','type']
df['taxid']=df['lineage'].apply(selectLast)

df.to_csv('/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/MetaPhlAnData/mpa_vJun23_CHOCOPhlAnSGB_202403_TAXID.tsv',sep='\t')