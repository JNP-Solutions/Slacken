import pandas as pd
import os

def getdataset(f_path):
    aFiles=os.listdir(f_path)
    aFiles=[f for f in aFiles if f.endswith('kreport.txt')]
    allTaxa=pd.DataFrame(columns=['Rank','Taxon'])
    for abundf in aFiles:
        aTemp=pd.read_csv(f_path+'/'+abundf, sep='\t',usecols=['Rank','Taxon'])
        aTemp.rename(columns={'Taxon':'NCBI_ID'})
        aTemp=aTemp[(aTemp['Rank'].isin(['S','S1','S2','S3','S4']))]
        allTaxa=pd.concat([allTaxa,aTemp])

        #print(merge['NCBI_ID'].unique())
    return pd.Series(allTaxa['Taxon'].unique())

def CheckDatasets(df_plantGenomes,df_marineGenomes,df_strainGenomes,df_assortedGenomes):
    df_std=pd.read_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/scripts/seq2Taxid/std/seqid2taxid.map",sep='\t', header=None)
    df_std.columns=['ID', 'NCBI_ID']

    df_rspc=pd.read_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/scripts/seq2Taxid/rspc/seqid2taxid.map", sep='\t', header=None)
    df_rspc.columns=['ID', 'NCBI_ID']

    df_metaphlan=pd.read_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/paper_figures/scripts/seq2Taxid/metaphlan-chocophlan/mpa_vJun23_CHOCOPhlAnSGB_202403_TAXID.tsv",sep='\t', usecols=['lineage','taxid'])
    df_metaphlan.columns=['ID', 'NCBI_ID']
    df_metaphlan=df_metaphlan[~df_metaphlan['NCBI_ID'].isna()]
    df_metaphlan['NCBI_ID']=df_metaphlan['NCBI_ID'].astype(int)


    #print(df_strainGenomes)

    #print(df_plantGenomes,df_marineGenomes,df_strainGenomes)

    for df, name in [(df_plantGenomes, "Plant"), (df_marineGenomes, "Marine"), (df_strainGenomes, "Strain"),(df_assortedGenomes, "Assorted_Genomes")]:
        std_intersection=pd.Series(df[df.isin(df_std['NCBI_ID'])].unique())
        df_std_uniq=pd.Series(df[~df.isin(df_std['NCBI_ID'])].unique())

        if(name=="Assorted_Genomes"):
            print(std_intersection,df_std_uniq,df.tolist())

        rspc_intersection=pd.Series(df[df.isin(df_rspc['NCBI_ID'])].unique())
        df_rspc_uniq=pd.Series(df[~df.isin(df_rspc['NCBI_ID'])].unique())

        metaphlan_intersection = pd.Series(df[df.isin(df_metaphlan['NCBI_ID'])].unique())
        df_metaphlan_uniq = pd.Series(df[~df.isin(df_metaphlan['NCBI_ID'])].unique())

        print(f"For {name} DB ------")
        print(f"Total Number of Distinct NCBI Taxa\t\t= {df.size}\n"
              f"Total Number of Taxa in std lib\t\t\t= {std_intersection.size}\n"
              f"Total Number of Taxa outside std lib\t= {df_std_uniq.size}\n"
              f"Total Number of Taxa in rspc lib\t\t= {rspc_intersection.size}\n"
              f"Total Number of Taxa outside rspc lib\t= {df_rspc_uniq.size}\n"
              f"Total Number of Taxa in metaphlan lib\t\t= {metaphlan_intersection.size}\n"
              f"Total Number of Taxa outside metaphlan lib\t= {df_metaphlan_uniq.size}\n")

    std_uniqNCBI=pd.Series(df_std['NCBI_ID'].unique())
    rspc_uniqNCBI=pd.Series(df_rspc['NCBI_ID'].unique())
    metaphlan_uniqNCBI=pd.Series(df_metaphlan['NCBI_ID'].unique())
    std_NCBI = pd.Series(df_std['NCBI_ID'])
    rspc_NCBI = pd.Series(df_rspc['NCBI_ID'])
    metaphlan_NCBI = pd.Series(df_metaphlan['NCBI_ID'])
    if(metaphlan_NCBI.size==metaphlan_uniqNCBI.size):
        print("yes, they're unique\n\n")
    else:
        print("NOT UNIQUE!!\n\n")

    print(f"Total Number of Distinct Taxa with assoc. genomes in std lib\t\t\t\t= {std_uniqNCBI.size}\n"
          f"Total Number of Distinct Genomes in std lib\t\t\t\t\t\t\t\t\t= {std_NCBI.size}\n"
          f"Total Number of Distinct Taxa with assoc. genomes in rspc lib\t\t\t\t= {rspc_uniqNCBI.size}\n"
          f"Total Number of Distinct Genomes in rspc lib\t\t\t\t\t\t\t\t= {rspc_NCBI.size}\n"
          f"Total Number of Distinct Taxa with assoc. genomes in metaphlan lib\t\t\t\t= {metaphlan_uniqNCBI.size}\n\n"
          f"Total Number of Distict Taxa with assoc. genomess unique to rspc lib\t\t= {len(rspc_uniqNCBI[~rspc_uniqNCBI.isin(std_uniqNCBI)])}\n"
          f"Total Number of Distinct Taxa with assoc. genomes unique to std lib\t\t\t= {len(std_uniqNCBI[~std_uniqNCBI.isin(rspc_uniqNCBI)])}\n"
          f"Total Number of Distinct Taxa with assoc. genomes unique to metaphlan lib v/s rspc\t\t\t= {len(metaphlan_uniqNCBI[~metaphlan_uniqNCBI.isin(rspc_uniqNCBI)])}\n"
          f"Total Number of Distinct Taxa with assoc. genomes unique to metaphlan lib v/s std\t\t\t= {len(metaphlan_uniqNCBI[~metaphlan_uniqNCBI.isin(std_uniqNCBI)])}")

# df_plantGenomes=pd.read_csv("/Users/n-dawg/Downloads/CAMI2_genomes/plant_associated/simulation_short_read/metadata.tsv", sep='\t', usecols=['NCBI_ID','novelty_category'])
# df_marineGenomes=pd.read_csv("/Users/n-dawg/Downloads/CAMI2_genomes/marine/simulation_short_read/metadata.tsv", sep='\t', usecols=['NCBI_ID','novelty_category'])
# df_strainGenomes=pd.read_csv("/Users/n-dawg/Downloads/CAMI2_genomes/strain/short_read/metadata.tsv", sep='\t', usecols=['NCBI_ID','novelty_category'])
#
# df_plantGenomes = df_plantGenomes[~df_plantGenomes['novelty_category'].isin(['new_genus', 'new_order', 'new_family'])]['NCBI_ID']
# df_marineGenomes = df_marineGenomes[~df_marineGenomes['novelty_category'].isin(['new_genus', 'new_order', 'new_family'])]['NCBI_ID']
# df_strainGenomes = df_strainGenomes[~df_strainGenomes['novelty_category'].isin(['new_genus', 'new_order', 'new_family'])]['NCBI_ID']
#
# df_plantGenomes.drop_duplicates(inplace=True, ignore_index=True)
# df_marineGenomes.drop_duplicates(inplace=True, ignore_index=True)
# df_strainGenomes.drop_duplicates(inplace=True, ignore_index=True)

df_plantGenomes=getdataset('/Users/n-dawg/Nextcloud/SBIshared/Slacken_Benchmark_plant_inSilico_marine/rspc_1-step_35_31_s7/plant_associated/mapping/')
df_marineGenomes=getdataset('/Users/n-dawg/Nextcloud/SBIshared/Slacken_Benchmark_marine/rspc_1-step-0--9_35_31_s7/marine/mapping/')
df_strainGenomes=getdataset('/Users/n-dawg/Nextcloud/SBIshared/Slacken_Strain0--50_Benchmark/rspc_1-step-0--50_35_31_s7/strain/mapping/')
df_assortedGenomes=getdataset('/Users/n-dawg/Nextcloud/SBIshared/Slacken_Benchmark_plant_inSilico_marine/rspc_1-step_35_31_s7/Assorted_Genomes_mbarc_225/mapping/')

CheckDatasets(df_plantGenomes,df_marineGenomes,df_strainGenomes,df_assortedGenomes)