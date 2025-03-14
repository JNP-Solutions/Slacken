import pandas as pd
import argparse
import re

# parser = argparse.ArgumentParser()
# parser.add_argument('--local_path', type=str, required=True, help='sample family output directory path')
# parser.add_argument('--sample_family', type=str, required=True, help='sample family name')
#
# args = parser.parse_args()
#
# localPath=args.local_path
# sampleFamily=args.sample_family

localPath="/Users/n-dawg/Nextcloud/SBIshared/MetaPhlAn4_outputs/"
sampleFamily="Assorted_Genomes_225"

familyCount={"strain":100,"marine":10,"plant_associated":20,"Assorted_Genomes_mbarc_225":10,
             "Assorted_Genomes_Perfect_225":10,"Assorted_Genomes_225":10}

sampleNames=["sample"+str(i) for i in range(0,familyCount[sampleFamily])]

bmetrics=pd.DataFrame(columns=["group","library","c","sample","LSE","LSE(log10)","L1","L1(log10)",
                              "TP","FP","FN","Precision","Recall","Added reads","rank"])

def extract_last_number(value):
    if value.endswith("|"):  # Check if the value ends in "|"
        return "ERROR: Ends with |"
        exit()
    match = re.search(r'(\d+)$', value)  # Extract last number
    return int(match.group(1)) if match else None

def makeList(df):
    df_filtered=df[df["#clade_name"].str.fullmatch(r'.*\|s__[^|]+')].copy()
    df_filtered["speciesId"] = df_filtered["clade_taxid"].apply(extract_last_number)
    speciesIdList = df_filtered[df_filtered["speciesId"] != "ERROR: Ends with |"]["speciesId"].tolist()
    return set(speciesIdList)

def makeCompare(sFamily,s):
    sMapping=pd.read_csv(localPath+sFamily+"/mapping/"+s+"_kreport.txt",sep="\t")
    sMetaphlan=pd.read_csv(localPath+sFamily+"/"+s+"/profiled_metagenome.txt",sep="\t",skiprows=5,)
    mappingTaxaList=set(sMapping[sMapping["Rank"]=='S']["Taxon"].tolist())
    metaphlanTaxaList=makeList(sMetaphlan.copy())
    #print(metaphlanTaxaList,mappingTaxaList,s)
    #exit()

    TP=set.intersection(mappingTaxaList,metaphlanTaxaList)
    FP=set.difference(metaphlanTaxaList,TP)
    FN=set.difference(mappingTaxaList,TP)

    return pd.DataFrame({"group": [sampleFamily], "library": ["MetaPhlAn4.1"], "c": ["N/A"],
                         "sample": [int(re.search(r'\d+$', s).group())], "LSE": ["N/A"], "LSE(log10)": ["N/A"],
                         "L1": ["N/A"], "L1(log10)": ["N/A"], "TP": [len(TP)], "FP": [len(FP)], "FN": [len(FN)],
                         "Precision": [float(len(TP))/len(metaphlanTaxaList)],
                         "Recall": [float(len(TP))/len(mappingTaxaList)], "Added reads": ["N/A"], "rank": ["Species"]})

for sample in sampleNames:
    print(makeCompare(sampleFamily,sample))
    bmetrics=pd.concat([bmetrics,makeCompare(sampleFamily,sample).reset_index(drop=True)])

bmetrics.to_csv(localPath+sampleFamily+"_metaphlan_bmetrics.tsv",sep="\t",index=False)