import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import matplotlib
matplotlib.use('Agg')

dfL1=pd.read_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/FigureData/slacken_VS_kraken2_L1_Distance.csv")

def add_figure4(dataSets, classifier_list, figShape, figName, rotn=30, thicken=0.2, twidth=0.15, addYlabel=True, decimalExp=False):
    print(dataSets)

    #fig, axes = plt.subplots(2, 2, figsize=(8, 6), dpi=300, sharex=True, sharey=False)  # 2x2 grid, share x-axis
    fig, axes = plt.subplots(2, 2, figsize=figShape, sharex=True, sharey=False)  # 2x2 grid, share x-axis

    plt.subplots_adjust(wspace=0.2, hspace=0.3)
    axes = axes.flatten()  # Flatten the 2x2 grid to easily iterate

    # Custom color scheme for scatter points
    colors_db_valid = {
        '(Pre-bracken) \nstd_1-step \nvs\n kraken2': '#CB4335',
        '(Post-bracken) \nstd_1-step \nvs\n kraken2': '#F1C40F', 
    }

    # Set alpha transparency for the scatter
    alphas = [0.5] * 12  # 12 classifiers

    coordId = 0

    for dataSet in dataSets:
        df_mm = dfL1
        df_mm = df_mm.rename(columns={'L1 (slacken 1-step vs kraken2)':'(Pre-bracken) \nstd_1-step \nvs\n kraken2'
            ,'L1 (slacken 1-step + bracken vs kraken2 + bracken)':'(Post-bracken) \nstd_1-step \nvs\n kraken2'})

        df_mm = df_mm[(df_mm['dataset'] == dataSet)]
        df = df_mm[['(Pre-bracken) \nstd_1-step \nvs\n kraken2','(Post-bracken) \nstd_1-step \nvs\n kraken2']]
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
            if(decimalExp==True):
                ax.text(i + twidth, median_val, f'{median_val:.5f}', color='black', fontsize=8, va='center', rotation=90)

            else:
                ax.text(i + twidth, median_val, f'{median_val:.2f}', color='black', fontsize=8, va='center', rotation=90)

        # Overlay scatter plots with jitter and custom colors
        for i, col in enumerate(df.columns):
            y = df[col]
            x = np.random.normal(i, 0.02, size=len(y))  # jitter x-axis for scatter
            ax.scatter(x, y, alpha=alphas[i], color=colors_db_valid[col], s=20)

        # Customize plot
        ax.set_xticks(range(len(df.columns)))
        ax.set_xticklabels(df.columns, fontsize=7, rotation=rotn, ha='center', fontweight='bold')  # Set x-axis labels
        ax.set_title(
            f'{dataSet} |  Size: ' + str(len(df_mm[df_mm['dataset']==dataSet]['sample'].unique())),
            fontweight='bold', fontsize=8)

    if(addYlabel==True):
        # Shared y-axis label
        fig.text(0.04, 0.5, var+" (species)", va='center', rotation='vertical', fontsize=10, fontweight='bold')

    # Tight layout to adjust subplot spacing and make room for the shared y-label
    plt.tight_layout(rect=[0.05,0,1,1])
    plt.savefig(f'../Figures/'+figName+".jpg",bbox_inches="tight", dpi=800)
    #plt.clf()

add_figure4(['strain', 'plant_associated', 'marine', 'Assorted_Genomes_mbarc_225'], ["std_1-step v/s kraken2", "std_1-step +braken v/s kraken2 + braken"], (6,6), 'bmetrics/slacken_1-step_vs_kraken2', rotn=0, thicken=0.27, twidth=0.17 ,addYlabel=False, decimalExp=True)
