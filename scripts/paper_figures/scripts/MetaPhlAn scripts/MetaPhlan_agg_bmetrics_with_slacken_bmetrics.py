import pandas as pd

df1=pd.read_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/benchmark_data/All_paper_bmetrics.tsv",sep='\t')
df2=pd.read_csv("/Users/n-dawg/Nextcloud/SBIshared/MetaPhlAn4_outputs/metaphlan_bmetrics.tsv",sep='\t')

df2.fillna("N/A")

pd.concat([df1,df2]).to_csv("/Users/n-dawg/IdeaProjects/Slacken-SBI/scripts/benchmark_data/"
                            "All_paper_bmetrics_with_metaphlan.tsv",sep='\t',index=False)