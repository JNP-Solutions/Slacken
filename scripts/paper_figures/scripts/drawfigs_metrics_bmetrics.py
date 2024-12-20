import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib
matplotlib.use('Agg')

df_metric=pd.read_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/benchmark_data/All_paper_metrics.tsv",sep='\t')
df_bmetric=pd.read_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/benchmark_data/All_paper_bmetrics.tsv",sep='\t')

def add_figure_single(rankx,dataSet,var,confd,classifier_list,inputType, figShape, figName, rotn=30, addYlabel=True):
    print(rankx,dataSet,var)
    if(inputType=='metrics'):
        df_input=df_metric
    elif(inputType=='bmetrics'):
        df_input=df_bmetric

    df_mm = df_input[['library', 'c', 'group', 'rank'] + [var]]
    df_mm = df_mm[(df_mm['group']== dataSet)]
    df_mm = df_mm[df_mm['c']==confd]
    df_mm = df_mm[(df_mm['rank'] == rankx)]

    df_mm = df_mm[df_mm['library'].isin(classifier_list)]

    df_mm=df_mm[['library',var]]
    #print(df_mm)
    df_mm=df_mm.groupby('library')[var].agg(list).reset_index()

    #print(list(zip(*df_mm[var].tolist())))
    df=pd.DataFrame(list(zip(*df_mm[var].tolist())), columns=df_mm['library'].tolist())
    df = df[classifier_list]

    # Custom color scheme for scatter points
    colors_db_valid = {
        'rspc_R100': '#9B59B6',
        'rspc_R10': '#5A5596',
        'rspc_R1': '#3C4A86',
        'rspc_1-step': '#1A5276',
        'std_R100': '#E67E22',
        'std_R10': '#D35400',
        'std_R1': '#DC3D1A',
        'std_1-step': '#CB4335',
        'kraken2': '#F1C40F',
        'rspc_gold': '#B7B7B7',
        'std_gold': '#B7B7B7',
    }

    # Set alpha transparency for the scatter
    alphas = [0.5, 0.5, 0.5, 0.5, 0.5,0.5, 0.5, 0.5]

    plt.figure(num=1, figsize=figShape, clear=True)


    # Plot boxplots with empty fill (transparent) and black boundaries
    for i, col in enumerate(df.columns):
        box = plt.boxplot(df[col], positions=[i], widths=0.2, patch_artist=True, showfliers=False)
        plt.setp(box['boxes'], facecolor='none', edgecolor='black', linewidth=1.2)  # No fill, black edges
        plt.setp(box['medians'], color='black', linewidth=1.2)
        plt.setp(box['whiskers'], color='black', linestyle='-', linewidth=1.2)
        plt.setp(box['caps'], color='black', linewidth=1.2)

        # Calculate the mean and print it next to the boxplot
        median_val = df[col].median()
        #print(median_val)
        plt.text(i + 0.15, median_val, f'{median_val:.2f}', color='black', fontsize=8, va='center',rotation=90)

    # Overlay scatter plots with jitter and custom colors
    for i, col in enumerate(df.columns):
        y = df[col]
        x = np.random.normal(i, 0.02, size=len(y))  # jitter x-axis for scatter
        plt.scatter(x, y, alpha=alphas[i], color=colors_db_valid[col], s=20)

    # Customize plot
    plt.xticks(range(len(df.columns)), df.columns, fontsize=9, rotation=rotn, fontweight='bold', ha='right')  # Set x-axis labels
    if(addYlabel==True):
        plt.ylabel(var + " (species)", fontweight='bold', fontsize=8)
    plt.tick_params(axis='y', labelsize=8)
    #plt.title('Dataset: '+dataSet +' |  Sample Size: ' + str(len(df_input[df_input['group']==dataSet]['sample'].unique())) + ' | Rank: ' + rankx, fontweight='bold', fontsize=8)
    #plt.title('Dataset: '+dataSet +'\n Sample Size: ' + str(len(df_input[df_input['group']==dataSet]['sample'].unique())) + '\nRank: ' + rankx, fontweight='bold', fontsize=7)
    plt.title(dataSet + ' |  Sample Size: ' + str(len(df_input[df_input['group'] == dataSet]['sample'].unique())), fontweight='bold',fontsize=8)
    plt.tight_layout(rect=[0.03,0,1,1])
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=400)

    #plt.clf()

def add_figure(rankx,dataSets,var,confd,classifier_list,inputType, figShape, figName):
    print(rankx,dataSets,var, confd, inputType)
    if(inputType=='metrics'):
        df_input=df_metric
    elif(inputType=='bmetrics'):
        df_input=df_bmetric

    coords=[(0,0),(0,1),(0,2),(1,0),(1,1),(1,2)]
    coordId=0

    #fig=plt.figure(figsize=(23, 12), clear=True)
    fig=plt.figure(figsize=figShape, clear=True)


    for dataSet in dataSets:

        df_mm = df_input[['library', 'c', 'group', 'rank'] + [var]]
        df_mm = df_mm[(df_mm['group'] == dataSet)]
        df_mm = df_mm[df_mm['c']==confd]
        df_mm = df_mm[(df_mm['rank'] == rankx)]
        df_mm = df_mm[df_mm['library'].isin(classifier_list)]
        df_mm=df_mm[['library',var]]
        df_mm=df_mm.groupby('library')[var].agg(list).reset_index()
        df=pd.DataFrame(list(zip(*df_mm[var].tolist())), columns=df_mm['library'].tolist())
        df = df[classifier_list]

        # Custom color scheme for scatter points
        colors_db_valid = {
            'rspc_R100': '#9B59B6',
            'rspc_R10': '#5A5596',
            'rspc_R1': '#3C4A86',
            'rspc_1-step': '#1A5276',
            'std_R100': '#E67E22',
            'std_R10': '#D35400',
            'std_R1': '#DC3D1A',
            'std_1-step': '#CB4335',
            'kraken2': '#F1C40F',
            'rspc_gold': '#B7B7B7',
            'std_gold': '#B7B7B7',
        }

        # Set alpha transparency for the scatter
        alphas = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]

        ax = plt.subplot2grid((2, 3), coords[coordId])
        coordId=coordId+1

        # Plot boxplots with empty fill (transparent) and black boundaries
        for i, col in enumerate(df.columns):
            box = ax.boxplot(df[col], positions=[i], widths=0.2, patch_artist=True, showfliers=False)
            plt.setp(box['boxes'], facecolor='none', edgecolor='black', linewidth=1.2)  # No fill, black edges
            plt.setp(box['medians'], color='black', linewidth=1.2)
            plt.setp(box['whiskers'], color='black', linestyle='-', linewidth=1.2)
            plt.setp(box['caps'], color='black', linewidth=1.2)

            # Calculate the mean and print it next to the boxplot
            median_val = df[col].median()
            #print(median_val)
            ax.text(i + 0.15, median_val, f'{median_val:.4f}', color='black', fontsize=8, va='center',rotation=90)

        # Overlay scatter plots with jitter and custom colors
        for i, col in enumerate(df.columns):
            y = df[col]
            x = np.random.normal(i, 0.02, size=len(y))  # jitter x-axis for scatter
            ax.scatter(x, y, alpha=alphas[i], color=colors_db_valid[col], s=20)

        # Customize plot
        plt.xticks(range(len(df.columns)), df.columns, fontsize=12)  # Set x-axis labels
        #plt.ylabel(var, fontweight='bold')
        plt.title('Dataset: '+dataSet +' |  Sample Size: ' + str(len(df_input[df_input['group']==dataSet]['sample'].unique())) + ' | Rank: ' + rankx, fontweight='bold', fontsize=10)

    #plt.savefig('/Users/n-dawg/Nextcloud/SBIshared/Slacken_graphs/latest/'+ dataSet +'/' + inputType +'/' + rankx + '/' + dataSet + '_' + var + '_' + rankx + '.jpg',
    #                dpi=400)
    fig.text(0.04, 0.5, var, va='center', rotation='vertical', fontsize=12, fontweight='bold')
    plt.tight_layout(rect=[0.05, 0, 1, 1])
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=800)
    #plt.clf()

def add_figure4(rankx, dataSets, var, confd, classifier_list, inputType, figShape, figName, rotn=30, thicken=0.2, twidth=0.15, addYlabel=True):
    print(rankx, dataSets, var, confd, inputType)

    if inputType == 'metrics':
        df_input = df_metric
    elif inputType == 'bmetrics':
        df_input = df_bmetric

    #fig, axes = plt.subplots(2, 2, figsize=(8, 6), dpi=300, sharex=True, sharey=False)  # 2x2 grid, share x-axis
    fig, axes = plt.subplots(2, 2, figsize=figShape, sharex=True, sharey=False)  # 2x2 grid, share x-axis

    plt.subplots_adjust(wspace=0.2, hspace=0.3)
    axes = axes.flatten()  # Flatten the 2x2 grid to easily iterate

    # Custom color scheme for scatter points
    colors_db_valid = {
        'rspc_R100': '#9B59B6',
        'rspc_R10': '#5A5596',
        'rspc_R1': '#3C4A86',
        'rspc_1-step': '#1A5276',
        'std_R100': '#E67E22',
        'std_R10': '#D35400',
        'std_R1': '#DC3D1A',
        'std_1-step': '#CB4335',
        'kraken2': '#F1C40F',
        'rspc_gold': '#B7B7B7',
        'std_gold': '#B7B7B7',
    }

    # Set alpha transparency for the scatter
    alphas = [0.5] * 11  # 11 classifiers

    coordId = 0

    for dataSet in dataSets:
        df_mm = df_input[['library', 'c', 'group', 'rank'] + [var]]
        df_mm = df_mm[(df_mm['group'] == dataSet)]
        df_mm = df_mm[df_mm['c'] == confd]
        df_mm = df_mm[(df_mm['rank'] == rankx)]
        df_mm = df_mm[df_mm['library'].isin(classifier_list)]
        df_mm = df_mm[['library', var]]
        df_mm = df_mm.groupby('library')[var].agg(list).reset_index()
        df = pd.DataFrame(list(zip(*df_mm[var].tolist())), columns=df_mm['library'].tolist())
        df = df[classifier_list]

        ax = axes[coordId]  # Get current axis
        coordId += 1

        # Plot boxplots with empty fill (transparent) and black boundaries
        for i, col in enumerate(df.columns):
            box = ax.boxplot(df[col], positions=[i], widths=thicken, patch_artist=True, showfliers=False)
            plt.setp(box['boxes'], facecolor='none', edgecolor='black', linewidth=1.2)  # No fill, black edges
            plt.setp(box['medians'], color='black', linewidth=1.2)
            plt.setp(box['whiskers'], color='black', linestyle='-', linewidth=1.2)
            plt.setp(box['caps'], color='black', linewidth=1.2)

            # Calculate the mean and print it next to the boxplot
            median_val = df[col].median()
            ax.text(i + twidth, median_val, f'{median_val:.2f}', color='black', fontsize=8, va='center', rotation=90)

        # Overlay scatter plots with jitter and custom colors
        for i, col in enumerate(df.columns):
            y = df[col]
            x = np.random.normal(i, 0.02, size=len(y))  # jitter x-axis for scatter
            ax.scatter(x, y, alpha=alphas[i], color=colors_db_valid[col], s=20)

        # Customize plot
        ax.set_xticks(range(len(df.columns)))
        ax.set_xticklabels(df.columns, fontsize=10, rotation=rotn, ha='right', fontweight='bold')  # Set x-axis labels
        ax.set_title(
            f'{dataSet} |  Size: ' + str(len(df_input[df_input['group']==dataSet]['sample'].unique())),
            fontweight='bold', fontsize=8)

    if(addYlabel==True):
        # Shared y-axis label
        fig.text(0.04, 0.5, var+" (species)", va='center', rotation='vertical', fontsize=10, fontweight='bold')

    # Tight layout to adjust subplot spacing and make room for the shared y-label
    plt.tight_layout(rect=[0.05,0,1,1])
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=800)
    #plt.clf()

def add_figure6(rankx, dataSets, var, confd, classifier_list, inputType, figShape, figName, rotn=30, thicken=0.2, twidth=0.15, addYlabel=True):
    print(rankx, dataSets, var, confd, inputType)

    if inputType == 'metrics':
        df_input = df_metric
    elif inputType == 'bmetrics':
        df_input = df_bmetric

    #fig, axes = plt.subplots(2, 2, figsize=(8, 6), dpi=300, sharex=True, sharey=False)  # 2x2 grid, share x-axis
    fig, axes = plt.subplots(2, 3, figsize=figShape, sharex=True, sharey=False)  # 2x2 grid, share x-axis

    plt.subplots_adjust(wspace=0.2, hspace=0.3)
    axes = axes.flatten()  # Flatten the 2x2 grid to easily iterate

    # Custom color scheme for scatter points
    colors_db_valid = {
        'rspc_R100': '#9B59B6',
        'rspc_R10': '#5A5596',
        'rspc_R1': '#3C4A86',
        'rspc_1-step': '#1A5276',
        'std_R100': '#E67E22',
        'std_R10': '#D35400',
        'std_R1': '#DC3D1A',
        'std_1-step': '#CB4335',
        'kraken2': '#F1C40F',
        'rspc_gold': '#B7B7B7',
        'std_gold': '#B7B7B7',
    }

    # Set alpha transparency for the scatter
    alphas = [0.5] * 11  # 11 classifiers

    coordId = 0

    for dataSet in dataSets:
        df_mm = df_input[['library', 'c', 'group', 'rank'] + [var]]
        df_mm = df_mm[(df_mm['group'] == dataSet)]
        df_mm = df_mm[df_mm['c'] == confd]
        df_mm = df_mm[(df_mm['rank'] == rankx)]
        df_mm = df_mm[df_mm['library'].isin(classifier_list)]
        df_mm = df_mm[['library', var]]
        df_mm = df_mm.groupby('library')[var].agg(list).reset_index()
        df = pd.DataFrame(list(zip(*df_mm[var].tolist())), columns=df_mm['library'].tolist())
        df = df[classifier_list]

        ax = axes[coordId]  # Get current axis
        coordId += 1

        # Plot boxplots with empty fill (transparent) and black boundaries
        for i, col in enumerate(df.columns):
            box = ax.boxplot(df[col], positions=[i], widths=thicken, patch_artist=True, showfliers=False)
            plt.setp(box['boxes'], facecolor='none', edgecolor='black', linewidth=1.2)  # No fill, black edges
            plt.setp(box['medians'], color='black', linewidth=1.2)
            plt.setp(box['whiskers'], color='black', linestyle='-', linewidth=1.2)
            plt.setp(box['caps'], color='black', linewidth=1.2)

            # Calculate the mean and print it next to the boxplot
            median_val = df[col].median()
            ax.text(i + twidth, median_val, f'{median_val:.2f}', color='black', fontsize=8, va='center', rotation=90)

        # Overlay scatter plots with jitter and custom colors
        for i, col in enumerate(df.columns):
            y = df[col]
            x = np.random.normal(i, 0.02, size=len(y))  # jitter x-axis for scatter
            ax.scatter(x, y, alpha=alphas[i], color=colors_db_valid[col], s=20)

        # Customize plot
        ax.set_xticks(range(len(df.columns)))
        ax.set_xticklabels(df.columns, fontsize=10, rotation=rotn, ha='right', fontweight='bold')  # Set x-axis labels
        ax.set_title(
            f'{dataSet} |  Size: ' + str(len(df_input[df_input['group']==dataSet]['sample'].unique())),
            fontweight='bold', fontsize=8)

    if(addYlabel==True):
        # Shared y-axis label
        fig.text(0.04, 0.5, var+" (species)", va='center', rotation='vertical', fontsize=10, fontweight='bold')

    # Tight layout to adjust subplot spacing and make room for the shared y-label
    plt.tight_layout(rect=[0.05,0,1,1])
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=500)
    #plt.clf()

def add_figure4PairedBox(rankx, dataSets, var1, var2, confd, classifier_list, inputType, figShape, figName, shade=False, rotateR=False, noYlabel=False, thicken=0.2):
    print(rankx, dataSets, var1, var2, confd, inputType)

    if inputType == 'metrics':
        df_input = df_metric
    elif inputType == 'bmetrics':
        df_input = df_bmetric

    fig, axes = plt.subplots(2, 2, figsize=figShape, sharex=True, sharey=False)  # 2x2 grid, share x-axis

    plt.subplots_adjust(wspace=0.2, hspace=0.3)
    axes = axes.flatten()  # Flatten the 2x2 grid to easily iterate

    # Custom color scheme for scatter points
    colors_db_valid = {
        'rspc_R100': '#9B59B6',
        'rspc_R10': '#5A5596',
        'rspc_R1': '#3C4A86',
        'rspc_1-step': '#1A5276',
        'std_R100': '#E67E22',
        'std_R10': '#D35400',
        'std_R1': '#DC3D1A',
        'std_1-step': '#CB4335',
        'kraken2': '#F1C40F',
        'rspc_gold': '#B7B7B7',
        'std_gold': '#B7B7B7',
    }

    # Set alpha transparency for the scatter
    alphas = [0.5] * 11  # 11 classifiers

    coordId = 0

    for dataSet in dataSets:
        df_mm1 = df_input[['library', 'c', 'group', 'rank', var1]]
        df_mm1 = df_mm1[(df_mm1['group'] == dataSet)]
        df_mm1 = df_mm1[df_mm1['c'] == confd]
        df_mm1 = df_mm1[(df_mm1['rank'] == rankx)]
        df_mm1 = df_mm1[df_mm1['library'].isin(classifier_list)]

        df_mm2 = df_input[['library', 'c', 'group', 'rank', var2]]
        df_mm2 = df_mm2[(df_mm2['group'] == dataSet)]
        df_mm2 = df_mm2[df_mm2['c'] == confd]
        df_mm2 = df_mm2[(df_mm2['rank'] == rankx)]
        df_mm2 = df_mm2[df_mm2['library'].isin(classifier_list)]

        df_mm1 = df_mm1[['library', var1]]
        df_mm1 = df_mm1.groupby('library')[var1].agg(list).reset_index()
        df1 = pd.DataFrame(list(zip(*df_mm1[var1].tolist())), columns=df_mm1['library'].tolist())

        df_mm2 = df_mm2[['library', var2]]
        df_mm2 = df_mm2.groupby('library')[var2].agg(list).reset_index()
        df2 = pd.DataFrame(list(zip(*df_mm2[var2].tolist())), columns=df_mm2['library'].tolist())

        df1 = df1[classifier_list]
        df2 = df2[classifier_list]
        # df = pd.concat([df1, df2], axis=1)  # Combine both variables in a single dataframe
        # df = df[classifier_list]

        ax = axes[coordId]  # Get current axis
        coordId += 1
        #shifter=0.3
        shifter=0.25
        bshifter=2
        # Plot boxplots with empty fill (transparent) and black boundaries
        for i, col in enumerate(df1.columns):
            box1 = ax.boxplot(df1[col], positions=[i*bshifter-shifter], widths=thicken, patch_artist=True, showfliers=False)
            plt.setp(box1['boxes'], facecolor='none', edgecolor='black', linewidth=1.2)  # No fill, black edges
            plt.setp(box1['medians'], color='black', linewidth=1.2)
            plt.setp(box1['whiskers'], color='black', linestyle='-', linewidth=1.2)
            plt.setp(box1['caps'], color='black', linewidth=1.2)

            # Calculate the mean and print it next to the boxplot
            median_val1 = df1[col].median()
            ax.text(i*bshifter + 0.25 - shifter, median_val1, f'{median_val1:.2f}', color='black', fontsize=8, va='center',
                    rotation=90)

        for i, col in enumerate(df2.columns):
            box2 = ax.boxplot(df2[col], positions=[i*bshifter+shifter], widths=thicken, patch_artist=True, showfliers=False)
            plt.setp(box2['boxes'], facecolor='none', edgecolor='black', linewidth=1.2)  # No fill, black edges
            plt.setp(box2['medians'], color='black', linewidth=1.2)
            plt.setp(box2['whiskers'], color='black', linestyle='-', linewidth=1.2)
            plt.setp(box2['caps'], color='black', linewidth=1.2)

            median_val2 = df2[col].median()
            ax.text(i*bshifter + 0.2 +shifter, median_val2, f'{median_val2:.2f}', color='black', fontsize=8, va='center', rotation=90)

        if(shade==True):
            ax.set_xlim(-0.8, 5)
            ax.axvspan(-0.8, 1, color='#e6e6fa', alpha=0.7, zorder=0)
            ax.axvspan(1, 5, color='#EBEBEB', alpha=0.2, zorder=0)
            #ax.axvspan(3, 5, color='#FBC9C6', alpha=0.7, zorder=0)
            ax.axvline(1, color='black', linestyle='--', linewidth=1, label='x=1')
            #ax.axvline(3, color='black', linestyle=':', linewidth=1, label='x=1')


        # Overlay scatter plots with jitter and custom colors
        for i, col in enumerate(df1.columns):
            y = df1[col]
            x = np.random.normal(i*bshifter-shifter, 0.02, size=len(y))  # jitter x-axis for scatter
            ax.scatter(x, y, alpha=alphas[i], color=colors_db_valid[col], s=20)

        for i, col in enumerate(df2.columns):
            y = df2[col]
            x = np.random.normal(i*bshifter+shifter, 0.02, size=len(y))  # jitter x-axis for scatter
            ax.scatter(x, y, alpha=alphas[i], color='#B7B7B7', s=20)

        # Customize plot
        rangeX=[i*bshifter for i in range(len(df1.columns))]
        #ax.set_xticks(range(len(df1.columns)))
        ax.set_xticks(rangeX)
        
        rotn = 90
        ha = 'right'
        if(rotateR == True):
            rotn = 0
            ha = 'center'

        ax.set_xticklabels(df1.columns, fontsize=10, rotation=rotn, ha=ha, fontweight='bold')  # Set x-axis labels
        ax.set_title(
            f'{dataSet} |  Size: ' + str(len(df_input[df_input['group'] == dataSet]['sample'].unique())),
            fontweight='bold', fontsize=7)

        # Shared y-axis label
        if(noYlabel==False):
            fig.text(0.04, 0.5, var1 +' & '+var2+ " (species)", va='center', rotation='vertical', fontsize=10, fontweight='bold')

    # Tight layout to adjust subplot spacing and make room for the shared y-label
    plt.tight_layout(rect=[0.05, 0, 1, 1])
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=600)
    # plt.clf()

# add_figure_single('Species','plant_associated','read_softIndex',0.15,["rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_R100", "std_R10", "std_R1", "std_1-step"],'metrics',(4,4),'metrics/plant_associated_read_softIndex_Species_0.15_std', 90, addYlabel=False)
# add_figure4('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'read_fp_frac', 0.15,["rspc_gold","rspc_R100", "rspc_1-step","std_gold" , "std_R100", "std_1-step"], 'metrics', (6,6), 'metrics/Species_all_read_fp_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure4('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'read_index', 0.15, ["rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics',(6,6),'metrics/Species_all_read_index_0.15',90, 0.3, 0.19, False)
# add_figure4('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'read_softIndex', 0.15, ["rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics',(6,6),'metrics/Species_all_read_softIndex_0.15', rotn=90, thicken=0.25, twidth=0.17, addYlabel=False)
# add_figure4('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'read_index', 0.15,["rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics',(10,6),'metrics/Species_all_read_index_0.15')
# add_figure4PairedBox('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'read_tp_frac','read_fp_frac', 0.15, ["rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics', (6,6), 'metrics/Species_all_read_tp_fracread_fp_frac_0.15_pairedBox', False, False, True, 0.4)
# add_figure4PairedBox('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'read_tp_frac','read_vp_frac', 0.15, ["rspc_1-step", "std_1-step"], 'metrics', (5,6), 'metrics/Species_all_read_tp_fracread_vp_frac_0.15_pairedBox', False, True)
# add_figure4PairedBox('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'read_tp_frac','read_vp_frac', 0.15, ["rspc_R100","rspc_1-step", "std_1-step"], 'metrics', (7,6), 'metrics/Species_all_read_tp_fracread_vp_frac_0.15_pairedBox_r100', True, True, True)
# add_figure4('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'L1', 0.15, ["rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_R100", "std_R10", "std_R1", "std_1-step"], 'bmetrics', (6,6), 'bmetrics/Species_all_L1_0.15', rotn=90, thicken=0.27, twidth=0.17 ,addYlabel=False)
# add_figure4('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], 'LSE', 0.15, ["rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_R100", "std_R10", "std_R1", "std_1-step"], 'bmetrics', (10,6), 'bmetrics/Species_all_LSE_0.15')



# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'read_tp_frac', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics', (10,6), 'supplementary_figures/Species_all_read_tp_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'read_fp_frac', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics', (10,6), 'supplementary_figures/Species_all_read_fp_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'read_vp_frac', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics', (10,6), 'supplementary_figures/Species_all_read_vp_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'read_fn_frac', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics', (10,6), 'supplementary_figures/Species_all_read_fn_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'read_index', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics', (10,6), 'supplementary_figures/Species_all_read_index_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'read_softIndex', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'metrics', (10,6), 'supplementary_figures/Species_all_read_softIndex_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'L1', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'bmetrics', (10,6), 'supplementary_figures/Species_all_read_L1_frac_0.15_gold', rotn=90, addYlabel=False)
# add_figure6('Species',['strain', 'plant_associated', 'marine', 'Assorted_Genomes_225', 'Assorted_Genomes_mbarc_225', 'Assorted_Genomes_Perfect_225'], 'LSE', 0.15,["rspc_gold", "rspc_R100", "rspc_R10", "rspc_R1", "rspc_1-step", "std_gold", "std_R100", "std_R10", "std_R1", "std_1-step"], 'bmetrics', (10,6), 'supplementary_figures/Species_all_read_LSE_frac_0.15_gold', rotn=90, addYlabel=False)
