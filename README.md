## Description
This is my showcase of an example data pipeline.
The data I will be extracting is squad/player statistics for the 2022-2023 Premier League (English Professional Football/Soccer League)
Website: https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures

## Process
Web scrapping -> Constructing Pandas Dataframe -> Uploading into MySQL Database

## Needed libs
Pandas
bs4
requests
random
time
os
glob
mysql.connector


## Step 1: Scrapping table data from the website
https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures
This is a great website to extract soccer data from.

Make a list of all match report links
(https://github.com/jpark184/datapipelineshowcase/blob/main/pic/1.png)
![alt text](https://github.com/jpark184/datapipelineshowcase/blob/main/pic/1.png "Logo Title Text 1")
