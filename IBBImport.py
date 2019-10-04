import csv
import io
from SQLConnector import SQLHelper

SQLHelper = SQLHelper()
x = 0
playerIDs = list()

with open('C:\\Users\\Herby\\Documents\\Python\\Baseball Stuff\\IBBs\\IBB-2017.csv') as IBBfile:
    data = csv.reader(IBBfile)
    iterData = iter(data)
    next(iterData)
    for row in iterData:
        playerName = row[0].split(" ",1)
        firstName = playerName[0]
        lastName = ""
        if(len(playerName)>1):
            lastName = playerName[1]
        numIBB = row[2]

        

        if(int(numIBB)>0):
            firstName = str(firstName).replace("'","''")
            lastName = str(lastName).replace("'","''")
            currPlayer = SQLHelper.select("select PlayerID from Players where FirstName='" + str(firstName) + "' and LastName='" + str(lastName) + "';")
            if(len(currPlayer)==1):
                for num in range(int(numIBB)):
                    playerIDs.append(int(currPlayer[0]["PlayerID"]))
            if(len(currPlayer)==0):
                inputID = input('Enter ID for ' + firstName + " " + lastName + ": ")
                for num in range(int(numIBB)):
                    playerIDs.append(int(inputID))
            if(len(currPlayer)>1):
                inputID = input('Enter ID for ' + firstName + " " + lastName + " who had " + numIBB + " IBBs: ")
                for num in range(int(numIBB)):
                    playerIDs.append(int(inputID))
        


for id in playerIDs:
    sql = "insert into Pitches (GameDate,BatterID,Event,EventDescription) values ('2017-02-02'," + str(id) + ",'intentional_walk','intentional_walk')"
    SQLHelper.insert(sql)
