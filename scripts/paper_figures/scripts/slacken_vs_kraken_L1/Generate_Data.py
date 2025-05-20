import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

masterDf=pd.DataFrame(columns=["dataset","sample","L1 (slacken 1-step vs kraken2)",
"L1 (slacken 1-step + bracken vs kraken2 + bracken)"])

masterDf.to_csv("/Users/n-dawg/Nextcloud/SBIshared/slacken_VS_kraken2_L1_Distance.csv",index=False)

# dfL1=pd.DataFrame(columns=["dataset","sample","L1 (slacken rspc 1-step vs kraken2",
#  "L1 (slacken rspc 1-step + bracken vs kraken2 + bracken)"])

def mergePandasPair(dataset, slackenPath, krakenPath,c15Dir, sampleNo, bracken):

    if(bracken==True):
        dfsB = pd.read_csv(slackenPath + dataset + "_v2/" + c15Dir + "/S" + str(
            sampleNo) + "_kreport_bracken_species.txt",sep='\t', header=None)

        dfkB = pd.read_csv(krakenPath + dataset + "_v2/kraken2_35_31_s7_c0.15_classified/S" + str(sampleNo) +
                           "_kreport_bracken_species.txt", sep='\t', header=None)

        dfsB.columns = ["#Perc", "S Aggregate", "In taxon", "Rank", "Taxon", "Name"]
        dfkB.columns = ["#Perc", "K Aggregate", "In taxon", "Rank", "Taxon", "Name"]

        df = pd.merge(dfsB[["S Aggregate", "Rank", "Taxon"]], dfkB[["K Aggregate", "Rank", "Taxon"]],
                         on=['Rank', 'Taxon'], how='outer')

        slackenreadCount=totalReadCount(dfsB,bracken, "S")
        krakenreadCount=totalReadCount(dfkB,bracken, "K")


    else:

        dfs = pd.read_csv(slackenPath + dataset + "_v2/" + c15Dir + "/S" + str(sampleNo) +
                          "_kreport.txt",sep='\t')

        dfk = pd.read_csv(krakenLoc + dataset + "_v2/kraken2_35_31_s7_c0.15_classified/S" + str(sampleNo) + "_kreport.txt",
                          sep='\t', header=None)

        dfs.rename(columns={"Aggregate": "S Aggregate"}, inplace=True)
        dfk.columns = ["#Perc", "K Aggregate", "In taxon", "Rank", "Taxon", "Name"]

        df = pd.merge(dfs[["S Aggregate", "Rank", "Taxon"]], dfk[["K Aggregate", "Rank", "Taxon"]],
                        on=['Rank', 'Taxon'], how='outer')

        slackenreadCount = totalReadCount(dfs, bracken, "S")
        krakenreadCount = totalReadCount(dfk, bracken, "K")

    # print((slackenreadCount,krakenreadCount), bracken)
    # if(bracken==False):
    #     if(slackenreadCount!=krakenreadCount):
    #         print(sampleNo, (slackenreadCount, krakenreadCount))
    return [df,(slackenreadCount,krakenreadCount)]

def L1Dist(df, readCounts):
    temp=df[df["Rank"]=='S'].copy()
    #print(temp["S In taxon"]-temp["K In taxon"])
    return ((temp["S Aggregate"]/(1.0*readCounts[0]))-(temp["K Aggregate"]/(1.0*readCounts[1]))).abs().sum()

def totalReadCount(df, bracken, sk):
    if(bracken==True):
        return df[df["Name"]=='root'][sk+" Aggregate"].tolist()[0]
    else:
        try:
            return df[df["Name"]=='root'][sk+" Aggregate"].tolist()[0]+df[df["Name"]=='unclassified'][sk+" Aggregate"].tolist()[0]
        except:
            return df[df["Name"]=='root'][sk+" Aggregate"].tolist()[0]

def writeToMaster(sampRange, slackenPath, slackenC15, sampInit=0):

    dfL1 = pd.DataFrame(columns=["dataset", "sample", "L1 (slacken 1-step vs kraken2)",
                                     "L1 (slacken 1-step + bracken vs kraken2 + bracken)"])

    for samp in range(sampInit, sampRange):
        dfsk, rcs = mergePandasPair(dataset, slackenPath, krakenLoc, slackenC15, samp, False)
        dfskB, rcsB = mergePandasPair(dataset, slackenPath, krakenLoc, slackenC15, samp, True)

        dfL1.loc[len(dfL1)] = [dataset, "S" + str(samp), L1Dist(dfsk, rcs), L1Dist(dfskB, rcsB)]

    masterDf = pd.read_csv("/Users/n-dawg/Nextcloud/SBIshared/slacken_VS_kraken2_L1_Distance.csv")

    masterDf = pd.concat([masterDf, dfL1])

    masterDf.to_csv("/Users/n-dawg/Nextcloud/SBIshared/slacken_VS_kraken2_L1_Distance.csv", index=False)




sPloc="/Users/n-dawg/Nextcloud/SBIshared/Slacken_Benchmark_plant_inSilico_marine/std_1-step_35_31_s7/"
sC15='std_1-step_35_31_s7_c0.15_classified'

krakenLoc="/Users/n-dawg/Nextcloud/SBIshared/Kraken_Benchmark/kraken2_35_31_s7/"

dataDict={
"plant_associated":[sPloc,sC15,20],

"Assorted_Genomes_mbarc_225":[sPloc,sC15, 10],

"Assorted_Genomes_225":[sPloc,sC15, 10],

"Assorted_Genomes_Perfect_225":[sPloc,sC15, 10],

"marine":["/Users/n-dawg/Nextcloud/SBIshared/Slacken_Benchmark_marine/std_1-step-0--9_35_31_s7/",
          "std_1-step-0--9_35_31_s7_c0.15_classified", 10],

"strain":[["/Users/n-dawg/Nextcloud/SBIshared/Slacken_Strain0--50_Benchmark/std_1-step-0--50_35_31_s7/",
              "/Users/n-dawg/Nextcloud/SBIshared/Slacken_Strain51--99_Benchmark/std_1-step_35_31_s7/"],
          ["std_1-step-0--50_35_31_s7_c0.15_classified","std_1-step_35_31_s7_c0.15_classified"], 10]
}

for dataset in ['plant_associated', 'marine', 'Assorted_Genomes_mbarc_225','Assorted_Genomes_225',
                'Assorted_Genomes_Perfect_225', 'strain']:

    if(dataset!='strain'):

        writeToMaster(dataDict[dataset][2],dataDict[dataset][0],dataDict[dataset][1])

    if(dataset=="strain"):
        print(dataDict[dataset][0][0])
        writeToMaster(51, dataDict[dataset][0][0], dataDict[dataset][1][0])
        writeToMaster(100,dataDict[dataset][0][1], dataDict[dataset][1][1], sampInit=51)