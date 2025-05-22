import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

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
    dfReduced = dfReduced.groupby('Group')['In taxon'].sum().reset_index()
    #print(dfReduced, dfReduced['In taxon'].sum())
    dfReduced['ratio'] = dfReduced['In taxon']/dfReduced['In taxon'].sum()
    #print(dfReduced,dfReduced['In taxon'].sum()/50)
    return dfReduced

def aggkreports(datasetPathsDict,classifierDict,classifiers,ds_name,noCollapse=False):
    dfResult=pd.DataFrame(columns=['classifier','In taxon','Group','ratio'])
    ds_paths=datasetPathsDict[ds_name]
    for clfName in classifiers:
        dfReduce_list = []
        for clf in classifierDict[clfName]:
            for pth in ds_paths:
                df_clf=pd.DataFrame(columns=['In taxon','Rank'])
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
                    else:
                        dfTemp=pd.read_csv(tempPath+fn,sep='\t',usecols=['In taxon', 'Rank'])
                    df_clf=pd.concat([df_clf,dfTemp])

                dfReduce_list.append(df_clf)
                if(clfName=='kraken'):
                    break
        dfCat=pd.concat(dfReduce_list)
        dfReduce=reduce(dfCat,noCollapse)
        dfReduce['classifier']=clfName
        dfResult=pd.concat([dfResult, dfReduce])
        #print(f'{clfName} :\t: {dfReduce}')
    return dfResult

def saveplot(result, dName, figName):
    pivot_df = result.pivot(index='classifier', columns='Group', values='ratio').fillna(0)
    #print(pivot_df)
    group_order = ['U', 'H', 'F', 'G', 'S']
    group_labels = {'S': 'Species', 'G': 'Genus', 'F': 'Family', 'H': 'Above Family', 'U': 'Unclassified'}
    colors = ['#B7B7B7', '#F8333C', '#FCAB10', '#2B9EB3', '#44AF69']

    pivot_df = pivot_df[group_order]
    # Plotting
    # fig, ax = plt.subplots()
    fig = plt.figure(facecolor="#fff3e0", figsize=(15, 12), dpi=300)  # figsize=(6, 2.5), dpi = 200
    ax = plt.subplot(facecolor="#fff3e0")

    pivot_df.plot(kind='bar', stacked=True, color=colors, edgecolor='black', ax=ax)
    ax.set_xlabel('Classifier', fontsize=15)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=0, fontsize=15)
    ax.set_ylabel('Fraction of Total Reads', fontsize=14)
    #ax.set_yticklabels(ax.get_yticklabels(),fontsize=15)
    ax.set_title(f'{dName}', fontsize=14)

    handles, labels = ax.get_legend_handles_labels()
    #print(handles, labels)
    new_label_order = ['S', 'G', 'F', 'H', 'U']
    new_labels = [group_labels[label] for label in new_label_order]
    new_handles = [handles[labels.index(label)] for label in new_label_order]
    #print(handles, new_handles)
    ax.legend(new_handles, new_labels, title='Classifications', bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=12)


    # Adjust spines for a cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.spines['bottom'].set_color('#DDDDDD')

    # Remove ticks for a cleaner appearance
    ax.tick_params(bottom=False, left=False)

    # Add horizontal grid lines with a light gray color, keeping vertical grids hidden
    ax.set_axisbelow(True)
    ax.yaxis.grid(False)
    ax.xaxis.grid(False)

    # Adjust layout to make room for the legend and ensure nothing is cut off
    fig.tight_layout()

    # Save the plot as an image file with the legend included
    #plt.savefig(f'/Users/n-dawg/Nextcloud/SBIshared/Slacken_graphs/All/multilevel_stacked_barcharts/{dName}.jpg',
    #            bbox_inches="tight", dpi=400)
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=400)
    #plt.show()

def saveplot4(result_list, dNames, figName, order):
    fig, axes = plt.subplots(2, 2, figsize=(15, 12), dpi=300, facecolor="#fff3e0", sharex=True, sharey=True)
    axes = axes.flatten()  # Flatten the 2x2 grid to iterate easily

    group_order = ['U', 'H', 'F', 'G', 'S']
    group_labels = {'S': 'Species', 'G': 'Genus', 'F': 'Family', 'H': 'Above Family', 'U': 'Unclassified'}
    colors = ['#B7B7B7', '#F8333C', '#FCAB10', '#2B9EB3', '#44AF69']

    def format_func(value, tick_number):
        return f'{value:.1f}'

    for i, (result, dName) in enumerate(zip(result_list, dNames)):
        pivot_df = result.pivot(index='classifier', columns='Group', values='ratio').fillna(0)
        pivot_df = pivot_df[group_order]  # Reorder columns
        
        pivot_df=pivot_df.reindex(order)

        ax = axes[i]
        # Disable legend for individual plots
        pivot_df.plot(kind='bar', stacked=True, color=colors, edgecolor='black', ax=ax, legend=False)

        ax.set_xlabel('Classifier', fontsize=13, fontweight='bold')
        ax.set_xticklabels(ax.get_xticklabels(), rotation=30, fontsize=14, fontweight='bold')

        #ax.set_ylabel('Fraction of Total Reads', fontsize=14, fontweight='bold')
        #ax.set_yticklabels(ax.get_yticks(), fontsize=12, fontweight='bold')
        ax.set_title(f'{dName}', fontsize=15, fontweight='bold')

        ax.yaxis.set_major_formatter(FuncFormatter(format_func))
        ax.set_yticklabels([f'{tick:.1f}' for tick in ax.get_yticks()], fontsize=12, fontweight='bold')


        # Remove spines and ticks
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_visible(False)
        ax.spines['bottom'].set_color('#DDDDDD')
        ax.tick_params(bottom=False, left=False)

        # Create a single legend in the top right corner of the figure
        handles, labels = axes[0].get_legend_handles_labels()
        new_label_order = ['S', 'G', 'F', 'H', 'U']
        new_labels = [group_labels[label] for label in new_label_order]
        new_handles = [handles[labels.index(label)] for label in new_label_order]
        legend = fig.legend(new_handles, new_labels, title='Classifications:',
                            loc='upper right', fontsize=12, frameon=True, prop={'weight': 'bold'})
        legend.get_title().set_fontweight('bold')

        # Customize the legend background to be opaque
        legend.get_frame().set_alpha(0.9)  # Set alpha to control opacity (1 is fully opaque)
        legend.get_frame().set_facecolor('white')  # Set the background color to white or any color you prefer

        # Adjust layout
        fig.tight_layout()

    #plt.savefig('/Users/n-dawg/Nextcloud/SBIshared/Slacken_graphs/All/multilevel_stacked_barcharts/multiple_datasets.jpg', bbox_inches="tight", dpi=400)
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=400)


#classifiers=['rspc_1-step','rspc_R100','rspc_R10','rspc_R1']
#classifiers=['rspc_1-step','rspc_R100','rspc_R10','rspc_R1','std_1-step','std_R100','std_R10','std_R1','kraken', 'rspc_gold', 'std_gold']
#classifiers=['std_1-step','std_R100','std_R10','std_R1']
#classifiers=['rspc_1-step','rspc_R100','rspc_R10','rspc_R1','std_1-step','std_R100','std_R10','std_R1']
#classifiers=['rspc_1-step','rspc_R100','std_1-step','std_R100', 'rspc_gold', 'std_gold']
dataset_list=list(names.keys())

# for dataset in dataset_list:
#     resultDf=aggkreports(names,name_classifier,classifiers, dataset)
#     saveplot(resultDf,dataset)

# order_1=['rspc_R100','rspc_R10','rspc_R1','rspc_1-step', 'std_R100','std_R10','std_R1', 'std_1-step']
# result_list_1 = [aggkreports(names, name_classifier, order_1, dataset) for dataset in dataset_list[:4]]  # Limit to 4 datasets
# saveplot4(result_list_1, dataset_list[:4],'multilevel_stacked_barcharts/multiple_datasets',order_1)
# df_result1= aggResultDf(result_list_1,dataset_list[:4])
# df_result1.to_csv("../FigureData/multiple_datasets.csv",index=False)

# order_2=['rspc_gold', 'rspc_R100', 'rspc_1-step', 'std_gold', 'std_R100', 'std_1-step']
# result_list_2 = [aggkreports(names, name_classifier, order_2, dataset) for dataset in dataset_list[:4]]  # Limit to 4 datasets
# saveplot4(result_list_2, dataset_list[:4],'multilevel_stacked_barcharts/multiple_datasets_gold',order_2)
# df_result2 = aggResultDf(result_list_2,dataset_list[:4])
# df_result2.to_csv("../FigureData/multiple_datasets_gold.csv",index=False)

# order_3=['rspc_gold', 'rspc_R100','rspc_R10','rspc_R1','rspc_1-step', 'std_gold', 'std_R100','std_R10','std_R1','std_1-step','kraken']
# result_list_3 = [aggkreports(names, name_classifier, order_3, dataset, noCollapse=True) for dataset in dataset_list]
# df_result3 = aggResultDf(result_list_3,dataset_list)
# df_result3.to_csv("../FigureData/all_datasets_NoFig.csv",index=False)

# order_4=['std_gold', 'std_1-step']
# result_list_4 = [aggkreports(names, name_classifier, order_4, dataset) for dataset in dataset_list[:4]]  # Limit to 4 datasets
# saveplot4(result_list_4, dataset_list[:4],'multilevel_stacked_barcharts/sbiF1',order_4)
# df_result4= aggResultDf(result_list_4,dataset_list[:4])
# df_result4.to_csv("../FigureData/sbiF1.csv",index=False)

order_5=['rspc_R100','rspc_R10','rspc_R1','rspc_1-step']
result_list_5 = [aggkreports(names, name_classifier, order_5, dataset) for dataset in dataset_list[:4]]  # Limit to 4 datasets
saveplot4(result_list_5, dataset_list[:4],'multilevel_stacked_barcharts/johan_talk_1',order_5)

order_6=['rspc_R100','rspc_1-step', 'std_R100', 'std_1-step']
result_list_6 = [aggkreports(names, name_classifier, order_6, dataset) for dataset in dataset_list[:4]]  # Limit to 4 datasets
saveplot4(result_list_6, dataset_list[:4],'multilevel_stacked_barcharts/johan_talk_2',order_6)