import pandas as pd
import argparse
import os

# parser = argparse.ArgumentParser()
# parser.add_argument('--local_path', type=str, required=True, help='sample family output directory path')
#
# args = parser.parse_args()
#
# localPath=args.local_path

localPath="/Users/n-dawg/Nextcloud/SBIshared/MetaPhlAn4_outputs/"

bfiles=os.listdir(localPath)
bfiles=[f for f in bfiles if f.endswith("bmetrics.tsv") and f!="metaphlan_bmetrics.tsv"]

bmetrics=pd.read_csv(localPath+bfiles[0],sep='\t')

for f in bfiles[1:]:
    bmetrics=pd.concat([bmetrics,pd.read_csv(localPath+f,sep='\t')])

bmetrics.to_csv(localPath+"metaphlan_bmetrics.tsv",sep='\t',index=False)
