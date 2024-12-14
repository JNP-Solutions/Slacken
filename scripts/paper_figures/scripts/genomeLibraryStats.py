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

def CheckDatasets(df_plantGenomes,df_marineGenomes,df_strainGenomes):
    df_std=pd.read_csv("./seq2Taxid/std/seqid2taxid.map",sep='\t', header=None)
    df_std.columns=['ID', 'NCBI_ID']

    df_rspc=pd.read_csv("./seq2Taxid/rspc/seqid2taxid.map", sep='\t', header=None)
    df_rspc.columns=['ID', 'NCBI_ID']

    #print(df_strainGenomes)

    #print(df_plantGenomes,df_marineGenomes,df_strainGenomes)

    for df, name in [(df_plantGenomes, "Plant"), (df_marineGenomes, "Marine"), (df_strainGenomes, "Strain")]:
        std_intersection=pd.Series(df[df.isin(df_std['NCBI_ID'])].unique())
        df_std_uniq=pd.Series(df[~df.isin(df_std['NCBI_ID'])].unique())

        rspc_intersection=pd.Series(df[df.isin(df_rspc['NCBI_ID'])].unique())
        df_rspc_uniq=pd.Series(df[~df.isin(df_rspc['NCBI_ID'])].unique())

        print(f"For {name} DB ------")
        print(f"Total Number of Distinct NCBI Taxa\t= {df.size}\n"
              f"Total Number of Taxa in std lib\t\t= {std_intersection.size}\n"
              f"Total Number of Taxa outside std lib\t= {df_std_uniq.size}\n"
              f"Total Number of Taxa in rspc lib\t= {rspc_intersection.size}\n"
              f"Total Number of Taxa outside rspc lib\t= {df_rspc_uniq.size}\n")

    std_uniqNCBI=pd.Series(df_std['NCBI_ID'].unique())
    rspc_uniqNCBI=pd.Series(df_rspc['NCBI_ID'].unique())
    std_NCBI = pd.Series(df_std['NCBI_ID'])
    rspc_NCBI = pd.Series(df_rspc['NCBI_ID'])

    print(f"Total Number of Distinct Taxa with assoc. genomes in std lib\t\t= {std_uniqNCBI.size}\n"
          f"Total Number of Distinct Genomes in std lib\t\t\t\t= {std_NCBI.size}\n"
          f"Total Number of Distinct Taxa with assoc. genomes in rspc lib\t\t= {rspc_uniqNCBI.size}\n"
          f"Total Number of Distinct Genomes in rspc lib\t\t\t\t= {rspc_NCBI.size}\n"
          f"Total Number of Distict Taxa with assoc. genomess unique to rspc lib\t= {len(rspc_uniqNCBI[~rspc_uniqNCBI.isin(std_uniqNCBI)])}\n"
          f"Total Number of Distinct Taxa with assoc. genomes unique to std lib\t= {len(std_uniqNCBI[~std_uniqNCBI.isin(rspc_uniqNCBI)])}")

df_plantGenomes=getdataset('/Users/n-dawg/Nextcloud/SBIshared/Slacken_Benchmark_plant_inSilico_marine/rspc_1-step_35_31_s7/plant_associated/mapping/')
df_marineGenomes=getdataset('/Users/n-dawg/Nextcloud/SBIshared/Slacken_Benchmark_marine/rspc_1-step-0--9_35_31_s7/marine/mapping/')
df_strainGenomes=getdataset('/Users/n-dawg/Nextcloud/SBIshared/Slacken_Strain0--50_Benchmark/rspc_1-step-0--50_35_31_s7/strain/mapping/')

CheckDatasets(df_plantGenomes,df_marineGenomes,df_strainGenomes)