import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(  # this is the credential to connect into mysql database
    host="########", user="#########", password="##########" #input your login details
)

connector = mydb.cursor()  # this is using the credential to create a connector

print("Field Data Upload Started")

deletequery = "truncate footballdatasets.field_data"  # this is query to reset the database
connector.execute(deletequery)  # I am executing the query

empdata = pd.read_csv(
    r"C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\CombinedData\fielddata.csv", #read the csv you want to upload
    index_col=False,
    delimiter=",",
)
empdata.fillna("null",inplace=True) #change all NA value into null
text = ""
for i in range(116): #create string to insert into sql query 116 is the number of columns
    text = text + "%s,"
text = text[:-1]

sql = f"INSERT INTO footballdatasets.field_data VALUES " \
      f"({text})"  # this is the query to insert data

for i,row,in empdata.iterrows():  # iterrows() and tuple(row) allows me to insert data row by row
    connector.execute(sql, tuple(row[1:]))  # row[1:] to not include the index
    # print(tuple(row[1:2]))
mydb.commit()  # commit the changes

print("Field Data Upload Completed")

print("Goalkeeper Data Upload Started")

deletequery = "truncate footballdatasets.goalkeeper_data"  # this is query to reset the database
connector.execute(deletequery)  # I am executing the query

empdata = pd.read_csv(
    r"C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\CombinedData\goalkeeperdata.csv",
    index_col=False,
    delimiter=",",
)#read the csv you want to upload

empdata.fillna("null",inplace=True)#change all NA value into null
text = ""
for i in range(28):
    text = text + "%s,"
text = text[:-1]

sql = f"INSERT INTO footballdatasets.goalkeeper_data VALUES " \
      f"({text})"  # this is the query to insert data

for i,row,in empdata.iterrows():  # iterrows() and tuple(row) allows me to insert data row by row
    connector.execute(sql, tuple(row[1:]))  # row[1:] to not include the index
    # print(tuple(row[1:2]))
mydb.commit()  # commit the changes
print("Goalkeeper Data Upload Completed")

print("End Process")
