import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import itertools

import seaborn as sns
import os

outer_path='/Users/n-dawg/Nextcloud/SBIshared/'
names={
    'strain' : ['Slacken_Strain0--50_Benchmark/','Slacken_Strain51--99_Benchmark/'],
    'marine' : ['Slacken_Benchmark_marine/'],
    'plant_associated' : ['Slacken_Benchmark_plant_inSilico_marine/'],
    'Assorted_Genomes_mbarc_225' : ['Slacken_Benchmark_plant_inSilico_marine/'],
    'Assorted_Genomes_225' : ['Slacken_Benchmark_plant_inSilico_marine/'],
    'Assorted_Genomes_Perfect_225' : ['Slacken_Benchmark_plant_inSilico_marine/']
}

name_classifier={
    'rspc_1-step' : ['rspc_1-step_35_31_s7', 'rspc_1-step-0--50_35_31_s7', 'rspc_1-step-0--9_35_31_s7'],
    'rspc_R1' : ['rspc_R1_35_31_s7', 'rspc_R1-0--50_35_31_s7', 'rspc_R1-0--9_35_31_s7'],
    'rspc_R10': ['rspc_R10_35_31_s7', 'rspc_R10-0--50_35_31_s7','rspc_R10-0--9_35_31_s7'],
    'rspc_R100': ['rspc_R100_35_31_s7', 'rspc_R100-0--50_35_31_s7', 'rspc_R100-0--9_35_31_s7'],
    'std_1-step': ['std_1-step_35_31_s7', 'std_1-step-0--50_35_31_s7', 'std_1-step-0--9_35_31_s7'],
    'std_R1': ['std_R1_35_31_s7', 'std_R1-0--50_35_31_s7', 'std_R1-0--9_35_31_s7'],
    'std_R10': ['std_R10_35_31_s7', 'std_R10-0--50_35_31_s7', 'std_R10-0--9_35_31_s7'],
    'std_R100': ['std_R100_35_31_s7', 'std_R100-0--50_35_31_s7', 'std_R100-0--9_35_31_s7'],
    'kraken': ['kraken2_35_31_s7'],
    'rspc_gold' : ['rspc_gold_35_31_s7', 'rspc_gold-0--50_35_31_s7', 'rspc_gold-0--9_35_31_s7'],
    'std_gold' : ['std_gold_35_31_s7', 'std_gold-0--50_35_31_s7', 'std_gold-0--9_35_31_s7']
}

addon_name='_c0.15_classified'

def aggResultDf(res_list,dNames):
    for (res, dName) in zip(res_list, dNames):
        res['dataset']=dName

    return pd.concat(res_list)

def collapse(value):
    if(value in ['R','K','D','P','C','O']):
        return 'H'
    else:
        return value

def reduce(df,noCollapse):
    rank_names=['U','R','K','D','P','C','O','F','G','S']
    dfReduced=df.copy()
    dfReduced['Group'] = df['Rank'].str.extract(r'([A-Z])')[0]
    if(noCollapse==False):
        dfReduced['Group'] = dfReduced['Group'].apply(collapse)
    dfReduced = dfReduced.groupby(['Group','sample'])['In taxon'].sum().reset_index()
    #print(dfReduced, dfReduced['In taxon'].sum())
    dfReduced['ratio'] = dfReduced.groupby('sample')['In taxon'].transform(lambda x: x / x.sum())
    #print(dfReduced,dfReduced['In taxon'].sum()/50)
    return dfReduced

def aggkreports(datasetPathsDict,classifierDict,classifiers,ds_name,noCollapse=False):
    dfResult=pd.DataFrame(columns=['classifier','In taxon','Group','ratio'])
    ds_paths=datasetPathsDict[ds_name]
    for clfName in classifiers:
        dfReduce_list = []
        for clf in classifierDict[clfName]:
            for pth in ds_paths:
                df_clf=pd.DataFrame(columns=['In taxon','Rank', 'sample'])
                if(clfName=='kraken'):
                    tempPath=outer_path+'Kraken_Benchmark/'+clf+'/'+ds_name+'_v2'+'/'+clf+addon_name+'/'
                else:
                    tempPath=outer_path+pth+clf+'/'+ds_name+'_v2'+'/'+clf+addon_name+'/'
                try:
                    filePaths=os.listdir(tempPath)
                except:
                    print(tempPath + " -- Doesn't exist")
                    continue

                fileNames=[fp for fp in filePaths if fp.endswith('_kreport.txt')]
                print(f'Parsing {len(fileNames)} Files...')
                for fn in fileNames:
                    if(clfName=='kraken'):
                        dfTemp=pd.read_csv(tempPath+fn,sep='\t',header=None)
                        dfTemp.columns=['#Perc','Aggregate','In taxon','Rank','Taxon','Name']
                        dfTemp=dfTemp[['In taxon', 'Rank']]
                        dfTemp['sample']=fn.split('_kreport')[0]
                    else:
                        dfTemp=pd.read_csv(tempPath+fn,sep='\t',usecols=['In taxon', 'Rank'])
                        dfTemp['sample']=fn.split('_kreport')[0]

                    df_clf=pd.concat([df_clf,dfTemp])

                dfReduce_list.append(df_clf)
                if(clfName=='kraken'):
                    break

        dfCat=pd.concat(dfReduce_list)
        dfReduce=reduce(dfCat,noCollapse)
        print(dfReduce)
        dfReduce['classifier']=clfName
        dfResult=pd.concat([dfResult, dfReduce])
        #print(f'{clfName} :\t: {dfReduce}')
    return dfResult

dataset_list=list(names.keys())

# for dataset in dataset_list:
#     resultDf=aggkreports(names,name_classifier,classifiers, dataset)
#     saveplot(resultDf,dataset)

result_list_3 = [aggkreports(names, name_classifier, ['rspc_1-step','rspc_R100','rspc_R10','rspc_R1','std_1-step','std_R100','std_R10','std_R1','kraken', 'rspc_gold', 'std_gold']
, dataset, noCollapse=True) for dataset in dataset_list]
df_result3 = aggResultDf(result_list_3,dataset_list)
df_result3.to_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/FigureData/"
                  "pairwise_difference_barcharts/all_datasets_NoFig.csv",index=False)

for ds in ['strain','marine','plant_associated','Assorted_Genomes_mbarc_225','Assorted_Genomes_225','Assorted_Genomes_Perfect_225']:
    for rank in ['S']:
        df = df_result3[(df_result3['dataset']==ds) & (df_result3['Group']==rank)]
        df = df[['classifier','sample','ratio']]

        # Step 1: Compute pairwise differences for each sample
        pairs = []
        for sample, group in df.groupby('sample'):
            for (clf1, ratio1), (clf2, ratio2) in itertools.combinations(group[['classifier', 'ratio']].values, 2):
                pairs.append({
                    'classifier_1': clf1,
                    'classifier_2': clf2,
                    'difference': ratio1 - ratio2,
                })

        pairs_df = pd.DataFrame(pairs)

        # Step 2: Compute the mean difference for each classifier pair
        mean_diff = pairs_df.groupby(['classifier_1', 'classifier_2'])['difference'].mean().reset_index()
        print(mean_diff)

        mean_diff.to_csv(f'/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/FigureData'
                           f'/pairwise_difference_barcharts/pairwise_meanOfDifferences_{ds}_{rank}.csv',index=False)