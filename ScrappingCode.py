import pandas as pd, bs4, requests,random,time

pd.set_option('display.max_columns', None) #pandas setting

#CREATING LIST OF MATCH REPORT LINKS
#----------------------------------------------------------------------------------#
URL = (r"https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures") #this is the website with match results
req = requests.get(URL) #requesting
print("Connection status:",req.status_code) #Checking Connection
soup = bs4.BeautifulSoup(req.text, 'html.parser') #Parsing HTML

tablesoup = soup.select('table.stats_table')[0] #the table with the link is named stats_table + table class
matchsoup = tablesoup.find_all('a') #all the links are under "<a"
matchlink = [] #empty list to add links to
for i in matchsoup:
    if i.text == "Match Report": #we only want links that are described as Match Report
        matchlink.append("http://www.fbref.com"+i.get("href"))
print("Match Link List Created")

#creating function that gets a link then outputs a two table as one dataframe csv file
#----------------------------------------------------------------------------------#
def dataExtract(link):
    matchurl = (link)  # this is the website with match results
    matchreq = requests.get(matchurl)  # requesting
    print("Starting new connection...")
    print("Connection status:", matchreq.status_code)  # Checking Connection

    #Really dumb way of getting home team and away team name + Date
    soup = bs4.BeautifulSoup(matchreq.text, 'html.parser')
    teamnamefinder = soup.select("h2")
    list1 = []
    for i in teamnamefinder:
        if "Player" in i.text:
            list1.append(i.text)
        else:
            pass
    TeamNames = list(set(list1))
    hometeam = TeamNames[0]
    awayteam = TeamNames[1]

    gamenamefinder = soup.select("h1")
    list2 = []
    for i in gamenamefinder:
        gamename = i.text

    print(f"Start Scrapping the game {gamename}") #shows what game we are scrapping

    #scrapping home team player stats
    #not using for loop for future reference
    #stat is divided in 7 different tables 6 for field players 1 for goalkeeper
    homefieldstat1 = pd.read_html(matchreq.text,match='Stats Table')[0]
    homefieldstat2 = pd.read_html(matchreq.text, match='Stats Table')[1]
    homefieldstat3 = pd.read_html(matchreq.text, match='Stats Table')[2]
    homefieldstat4 = pd.read_html(matchreq.text, match='Stats Table')[3]
    homefieldstat5 = pd.read_html(matchreq.text, match='Stats Table')[4]
    homefieldstat6 = pd.read_html(matchreq.text, match='Stats Table')[5]

    homegoalkeeperstat = pd.read_html(matchreq.text, match='Stats Table')[6]

    homefieldstat = pd.concat([homefieldstat1,homefieldstat2.iloc[:,6:],homefieldstat3.iloc[:,6:],homefieldstat4.iloc[:,6:],
                               homefieldstat5.iloc[:,6:],homefieldstat6.iloc[:,6:]],axis=1) #combining dataframes into one dataframe

    homegoalkeeperstat.columns = homegoalkeeperstat.columns.map('|'.join).str.strip('|')  # flattening the multiindex column
    homegoalkeeperstat["Home|Away"] = "Home" #adding home away
    homegoalkeeperstat["Team"] = hometeam  # adding team name

    homefieldstat.columns = homefieldstat.columns.map('|'.join).str.strip('|') #flattening the multiindex column
    homefieldstat["Home|Away"] = "Home" #adding home away
    homefieldstat["Team"] = hometeam  # adding team name

    #scrapping away team player stats
    awayfieldstat1 = pd.read_html(matchreq.text,match='Stats Table')[7]
    awayfieldstat2 = pd.read_html(matchreq.text, match='Stats Table')[8]
    awayfieldstat3 = pd.read_html(matchreq.text, match='Stats Table')[9]
    awayfieldstat4 = pd.read_html(matchreq.text, match='Stats Table')[10]
    awayfieldstat5 = pd.read_html(matchreq.text, match='Stats Table')[11]
    awayfieldstat6 = pd.read_html(matchreq.text, match='Stats Table')[12]

    awaygoalkeeperstat = pd.read_html(matchreq.text, match='Stats Table')[13]

    awayfieldstat = pd.concat([awayfieldstat1,awayfieldstat2.iloc[:,6:],awayfieldstat3.iloc[:,6:],awayfieldstat4.iloc[:,6:],
                               awayfieldstat5.iloc[:,6:],awayfieldstat6.iloc[:,6:]],axis=1) #combining dataframes into one dataframe

    awaygoalkeeperstat.columns = awaygoalkeeperstat.columns.map('|'.join).str.strip('|')  # flattening the multiindex column
    awaygoalkeeperstat["Home|Away"] = "Away"  # adding home away
    awaygoalkeeperstat["Team"] = awayteam  # adding team name
    awayfieldstat.columns = awayfieldstat.columns.map('|'.join).str.strip('|') #flattening the multiindex column
    awayfieldstat["Home|Away"] = "Away"  # adding home away
    awayfieldstat["Team"] = awayteam  # adding team name

    #Combining Home and Away dataframe into one
    fieldstat = pd.concat([homefieldstat.iloc[:-1],awayfieldstat.iloc[:-1]],axis = 0) #we don't need the last row of each dataframe
    fieldstat["Game"] = gamename #Adding date
    goalkeeperstat = pd.concat([homegoalkeeperstat,awaygoalkeeperstat],axis = 0)
    goalkeeperstat["Game"] = gamename #Adding date
    print("Dataframe is ready...Exporting as csv") #dataframe ready signal

    space="_"
    fieldstat.to_csv(rf"C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\Fieldstat\fieldstate_{gamename}.csv"
                     ,encoding='utf-8-sig') #ExportingData
    goalkeeperstat.to_csv(rf"C:\Users\justi\OneDrive\Desktop\ScrapAndPipeline\Data\Goalkeeperstat\goalkeeperstat_{gamename}.csv"
                          ,encoding='utf-8-sig') # ExportingData
    print("Export completed")
    print("-------------------------------------------------------------------------------------------------------------------------------------------------") #end of scrap

for i in matchlink:
    dataExtract(i)
    print("Sleeping for a given time...")
    time.sleep(random.randint(7,13)) #this is avoid security detection
    print("I am awake!")

print("End process")