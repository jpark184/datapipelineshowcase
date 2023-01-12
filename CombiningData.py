import pandas as pd
import os
import glob
pd.set_option('display.max_columns', None) #pandas setting

Fieldstat = (r'C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\Fieldstat') #Input source data folder
Fieldstatcsv = glob.glob(os.path.join(Fieldstat, "*.csv")) #This gets all the csv file in the source data folder

Goalkeeperstat = (r'C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\Goalkeeperstat') #Input source data folder
Goalkeeperstatcsv = glob.glob(os.path.join(Goalkeeperstat, "*.csv")) #This gets all the csv file in the source data folder


#Combining FieldStat CSV Files
#----------------------------------------------------------------------------------#
df1 = pd.read_csv(Fieldstatcsv[0]) #Get a base csv
n = 0
for i in Fieldstatcsv:
    if n==0:
        pass
    else:
        df2 = pd.read_csv(i)
        df1 = pd.concat([df1,df2]) #combine base df to df we are currently reading
    n+=1
    print(f"Combining{n}th csv file")
print("Combining FieldStat CSV Completed")
df1.to_csv(r'C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\CombinedData\fielddata.csv',encoding='utf-8-sig') #export combined df

#Combining GoalKeeperStat CSV Files
#----------------------------------------------------------------------------------#
df1 = pd.read_csv(Goalkeeperstatcsv[0]) #Get a base csv
n = 0
for i in Goalkeeperstatcsv:
    if n==0:
        pass
    else:
        df2 = pd.read_csv(i)
        df1 = pd.concat([df1,df2]) #combine base df to df we are currently reading
    n+=1
    print(f"Combining{n}th csv file")
print("Combining Goalkeeperstat CSV Completed")
df1.to_csv(r'C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\CombinedData\goalkeeperdata.csv',encoding='utf-8-sig')
