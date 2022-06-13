# python3.8

import random
import mariadb
import sys
import time

from paho.mqtt import client as mqtt_client
from datetime import datetime

try:
    conn=mariadb.connect(
            user="Serveur",
            password="123Soleil",
            host="127.0.0.1",
            port=3306,
            database="GTHSMEN",
            )
except mariadb.Error as e :
    print(f"Error connectng to MariaDB Platform ; {e}")
    sys.exit(1)
conn.autocommit =True
cur = conn.cursor()
cur2 =conn.cursor()

broker = '51.255.47.95'
port = 1883 
topics = ["newplayer"]
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 100)}'
username = 'Serveur'
password = '123Soleil'
lastNGame = ""
def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")

        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def publish(client,topicUp,msgUp):
         
         result = client.publish(topicUp, msgUp)
         # result: [0, 1]
         status = result[0]
         if status == 0:
             print(f"Send `{msgUp}` to topic `{topicUp}`")
         else:
             print(f"Failed to send message to topic {topicUp}")
         
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        time.sleep(0.5)
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        if msg.topic == "newplayer" and checkFormMsg(msg.payload.decode(),"np"):
            generateID(msg.payload.decode())
        elif checkFormMsg(msg.payload.decode(),"oth"):
            sliced_msg=slicerMessage(msg)
            maria_query='SELECT IdJ1,IdJ2,Waiting4,Status,LastMsgUp,LastMsgDown FROM Games WHERE IdGame="'+sliced_msg[0]+'"'
            cur.execute(maria_query)    
            infoGame=["","","","","",""]
            firstPlayer=""
            for (IdJ1,IdJ2,Waiting4,Status,LastMsgUp,LastMsgDown)  in cur:
                infoGame[0]=IdJ1
                infoGame[1]=IdJ2    
                infoGame[2]=Waiting4
                infoGame[3]=Status
                infoGame[4]=LastMsgUp
                infoGame[5]=LastMsgDown
            msgWin=""

            if sliced_msg[1][1:2]=='1':
                 pubPlayer=2
                 msgWin="G98"
            else:
                 msgWin="G99"
                 pubPlayer=1
            pubPlayer=0;
            if sliced_msg[2]=="G94" and int(sliced_msg[1][1:2]) == infoGame[2]: #donnée envoyé au module erronée
                publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In",infoGame[4])
                updtTable("Games",sliced[0],["IntervalTime","Timestamp"],[60,dateNow()])
            elif sliced_msg[2]=="G96" and int(sliced_msg[1][1:2]) != infoGame[2] and infoGame[3] != "STARTING":
                publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G05")
            elif sliced_msg[2]=="G25" and infoGame[3] != "STARTING": #abandon du joueur
                 publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In",msgWin)
                 publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In",msgWin)
                 updtTable("Games",sliced_msg[0],["Status","Waiting4","LastMsgUp","LastMsgDown","IntervalTime","Timestamp"],["FINISHING",0,msgWin,"G25",60,dateNow()])
                    
            elif infoGame[3]== 'STARTING' :
                if infoGame[2]== 0 and( sliced_msg[1] == infoGame[0] or sliced_msg[1] == infoGame[1]):
                    if sliced_msg[2] == "G01":
                        publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G05")
                        updtTable("Games",sliced_msg[0],["Waiting4","LastMsgUp","LastMsgDown"],[2,"G05","G01"])
                    elif sliced_msg[2] == "G02":
                        publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G05")
                        updtTable("Games",sliced_msg[0],["Waiting4","LastMsgUp","LastMsgDown"],[1,"G05","G02"])
                    elif sliced_msg[2] == "G00" and infoGame[4]=="G03":
                        if sliced_msg[1][0:2] == "M1":
                            updtTable("Games",sliced_msg[0],["Waiting4","LastMsgDown"],[2,"G00"])
                        elif sliced_msg[1][0:2] == "M2":
                            updtTable("Games",sliced_msg[0],["Waiting4","LastMsgDown"],[1,"G00"])

                elif infoGame[2] == 1 and sliced_msg[1] == infoGame[0]:
                    if sliced_msg[2] == "G00" and infoGame[4]=="G03":
                        firstPlayer=random.randint(0,1)
                        if(firstPlayer==0):
                            pubPlayer=1
                            publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G04")
                        elif(firstPlayer==1):
                            pubPlayer=0
                            publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G04")
                        publish(client,sliced_msg[0]+"/"+infoGame[pubPlayer]+"/In","G07")
                        updtTable("Games",sliced_msg[0],["Waiting4","LastMsgUp","LastMsgDown","Status","IntervalTime","Timestamp"],[firstPlayer+1,"G04","G00","PLAYING",60,dateNow()])
                    elif sliced_msg[2] == "G01" and infoGame[5]=="G02":
                        publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G05")
                       
                        publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G03")
                        time.sleep(1)
                        publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G03")
                        updtTable("Games",sliced_msg[0],["Waiting4","LastMsgUp","LastMsgDown","IntervalTime","Timestamp"],[0,"G03","G02",300,dateNow()])
                elif infoGame[2]==2 and sliced_msg[2]=="G08":
                    publish(client,"TGF","G08")
                elif infoGame[2] == 2 and sliced_msg[1] == infoGame[1]:
                    if sliced_msg[2] == "G00" and infoGame[4]=="G03":
                        firstPlayer=random.randint(0,1)
                        if(firstPlayer==0):
                            publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G04")
                            pubPlayer=1
                        elif(firstPlayer==1):
                            pubPlayer=0
                            publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G04")
                        publish(client,sliced_msg[0]+"/"+infoGame[pubPlayer]+"/In","G07")
                        updtTable("Games",sliced_msg[0],["Waiting4","LastMsgUp","LastMsgDown","Status","IntervalTime","Timestamp"],[firstPlayer+1,"G04","G00","PLAYING",60,dateNow()])
                    elif sliced_msg[2] == "G02" and infoGame[5]=="G01":
                        publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G05")
                        
                        publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G03")
                        time.sleep(1)
                        publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G03")
                        updtTable("Games",sliced_msg[0],["Waiting4","LastMsgUp","LastMsgDown","IntervalTime","Timestamp"],[0,"G03","G02",300,dateNow()])

            elif infoGame[3]=="PLAYING":
                if int(sliced_msg[1][1]) == infoGame[2]:
                    if sliced_msg[2][0]=='T' and infoGame[4]=="G04":
                        if int(sliced_msg[1][1])==1:
                            pubPlayer = 2
                        else:
                            pubPlayer =1
                        print(pubPlayer,sliced_msg[1][1])
                        publish(client,sliced_msg[0]+"/"+infoGame[pubPlayer-1]+"/In","G22")
                        updtTable("Games",sliced_msg[0],["Waiting4","Status","LastMsgUp","LastMsgDown","IntervalTime","Timestamp"],[pubPlayer,"CHECKING","G22",sliced_msg[2],60,dateNow()])
            elif infoGame[3]=="CHECKING":
                if int(sliced_msg[1][1]) == infoGame[2]:
                    if sliced_msg[2]=="G24": #flotte coulée
                        publish(client,sliced_msg[0]+"/"+infoGame[pubPlayer]+"/In","G24")
                        updtTable("Games",sliced_msg[0],["LastMsgDown","IntervalTime","Status","Timestamp"],["G24",60,"FINISHING",dateNow()])

                    elif sliced_msg[2] == "G23" and infoGame[4] == "G22":
                        publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In",infoGame[5])
                        if infoGame[5][0] == 'M':
                            if sliced_msg[1][1] == '2':
                                pubPlayer = 1
                                publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G04")
                                publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G07")
                            else :
                                pubPlayer = 2
                                publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G04")
                                publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G07")
                      
                            updtTable("Games",sliced_msg[0],["LastMsgUp","LastMsgDown","Waiting4","Status","IntervalTime","Timestamp"],["G04","G23",pubPlayer,"PLAYING",60,dateNow()])
                        elif infoGame[5][0] in "HS":
                            if sliced_msg[1][1] == '1':
                                pubPlayer = 1
                                publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G04")
                                publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G07")
                            else :
                                pubPlayer = 2
                                publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In","G04")
                                publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In","G07")
                        
                            updtTable("Games",sliced_msg[0],["LastMsgUp","LastMsgDown","Waiting4","Status","IntervalTime","Timestamp"],["G04","G23",pubPlayer,"PLAYING",60,dateNow()]) 
                        else :      
                            updtTable("Games",sliced_msg[0],["LastMsgUp","LastMsgDown","IntervalTime","Timestamp"],[infoGame[5],"G23",60,dateNow()])
                    
                    elif  sliced_msg[2][0] in "HSM" and infoGame[5]=="G23" :
                        if sliced_msg[1][1] == '1':

                            pubPlayer = 2
                        else :
                            pubPlayer = 1
                        publish(client,sliced_msg[0]+"/"+infoGame[pubPlayer-1]+"/In","G22")
                        updtTable("Games",sliced_msg[0],["LastMsgUp","LastMsgDown","IntervalTime","Timestamp","Waiting4"],["G22",sliced_msg[2],60,dateNow(),pubPlayer])
            elif infoGame[3]=="FINISHING" and (infoGame[2]==int(sliced_msg[1][1]) or infoGame[2]==0):
                receiver=0
                sender=int(sliced_msg[1][1])
                if sender == 1:
                    receiver=2
                else:
                    receiver=1          
                if sliced_msg[2][0] == 'S' and infoGame[5]=="G24":
                    publish(client,sliced_msg[0]+"/"+infoGame[receiver-1]+"/In","G22")
                    updtTable("Games",sliced_msg[0],["LastMsgUp","LastMsgDown","Waiting4","IntervalTime","Timestamp"],["G22",sliced_msg[2],receiver,60,dateNow()])
                elif sliced_msg[2] == "G23" and infoGame[4]=="G22":
                    publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In",infoGame[5])
                    time.sleep(1)
                    publish(client,sliced_msg[0]+"/"+infoGame[0]+"/In",msgWin)
                    publish(client,sliced_msg[0]+"/"+infoGame[1]+"/In",msgWin)
                    updtTable("Games",sliced_msg[0],["LastMsgDown","LastMsgUp","Waiting4","IntervalTime","Timestamp"],["G23",msgWin,0,60,dateNow()])
                elif sliced_msg[2] == "G44" and infoGame[4] in ["G99","G98"] :
                   publish(client,sliced_msg[0]+"/"+infoGame[receiver-1]+"/In","G44")
                   updtTable("Games",sliced_msg[0],["LastMsgUp","IntervalTime","Timestamp"],["G44",60,dateNow()])
                elif sliced_msg[2] == "G45" and infoGame[4]=="G44":
                   publish(client,sliced_msg[0]+"/"+infoGame[receiver-1]+"/In","G45")
                   updtTable("Games",sliced_msg[0],["LastMsgUp","IntervalTime","Timestamp"],["G45",60,dateNow()])
                elif sliced_msg[2][0:1] =='T' and infoGame[4]=="G44":
                    publish(client,sliced_msg[0]+"/"+infoGame[receiver-1]+"/In",sliced_msg[2])
                elif sliced_msg[2] =="G06" and infoGame[4] in ["G99","G98"]:
                    if infoGame[5]=="G06" :
                        newGame(sliced_msg,infoGame)
                    else:
                        publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G06")
                        updtTable("Games",sliced_msg[0],["LastMsgDown","Waiting4","IntervalTime","Timestamp"],["G06",receiver,120,dateNow()])
            else:
                publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G95")
        else:
            sliced_msg=slicerMessage(msg)
            publish(client,sliced_msg[0]+"/"+sliced_msg[1]+"/In","G95")
                        
    for topic in topics:
        client.subscribe(topic)
        print(f"Subscribed to `{topic}`")
    client.on_message = on_message
def dateNow():
    return datetime.timestamp(datetime.now())

def checkMAC(MAC):
    compMac=convMac(MAC)
    l=open("logId.txt","r")
    lines=l.readlines()
    unassigned=True
    party=""
    idAssigned=""
    for line in lines:
        if compMac+" assigned" in line:
            unassigned=False
            idAssigned=line[line.index(compMac)-2:line.index(compMac)+12]
            party=line[line.index('P'):len(line)-2]
        elif compMac+" unassigned" in line:
            indexLine=0
            unassigned=True
    l.close()    
    if not unassigned:
        publish(client,MAC,str(party)+"/"+str(idAssigned)+"/In")
    return unassigned

def convMac(mac):
    convId=""
    conversionTable=[0,'Z',2,'Y',4,'X',6,'W',8,'V',5,'U',3,'T',1,'S']
    for i in range(0,len(mac)):
        if mac[i]!=':':
            convId=convId+str(conversionTable[int(mac[i],16)]) 
    return convId

def checkFormMsg(msg,mod):
    lnMsg=len(msg)  
    if lnMsg == 3 and mod!="np":
        if msg[0] in "HGSMT" and int(msg[1:3])>=0 and int(msg[1:3])<=99:
                return True
    elif lnMsg==17 and mod=="np":
        for i in range(0,6): 
            if msg[i*3] not in "ABCDEFabcdef0123456789" and msg[(i*3)+1] not in "ABCDEFabcdef0123456789":
                return False
        if checkMAC(msg):
            return True
        else:
            return False
    else:
        return False

def newGame(resetGame,infoGameReset):
    publish(client,resetGame[0]+"/"+infoGameReset[0]+"/In","G03")
    publish(client,resetGame[0]+"/"+infoGameReset[1]+"/In","G03")
    updtTable("Games",resetGame[0],["Status","Waiting4","LastMsgUp","LastMsgDown","Timestamp","IntervalTime"],["STARTING",0,"G03","G06",dateNow(),300])

def generateID(msg):
    slicedTemp=""
    global lastNGame
    l=open("logId.txt","r")
    lines=l.readlines()
    l.close()
    nextPlayer="M1"
    for line in lines:
        if "M1" in line and " assigned " in line :
            nextPlayer="M2"
        elif "M2" in line and " assigned " in line:
            nextPlayer="M1"
    if nextPlayer=="M1":
        nGame = generateGame()
        lastNGame = nGame
    else:
        nGame=lastNGame 
    slicedTemp=nextPlayer+convMac(msg)
    topics.append(f"{nGame}/{slicedTemp}/Out")
    writeLog("logId.txt",slicedTemp+ " assigned to "+nGame)
    if slicedTemp[0:2]=="M1":
       updtTable("Games",nGame,["IdJ1"],[slicedTemp])
    else:
       updtTable("Games",nGame,["IdJ2"],[slicedTemp])
    subscribe(client)
    publish(client,msg,lastNGame+"/"+slicedTemp+"/In")
    publish(client,"test/reception",lastNGame+"/"+slicedTemp)
   
def generateGame():
    now = datetime.now()
    idParty ="P"+("%i%i%i%i%i%i"%(now.day,now.year,now.hour,now.second,now.month,now.minute))
    addValues('Games',"IdGame,Waiting4,Status","'"+idParty+"',0,'STARTING'")
    return idParty
    
def slicerMessage(msg):
    player=""
    game=""
    slash=True
    payload=msg.payload.decode()
    for i in range(0,len(msg.topic)-4):
        if msg.topic[i] =='/':
            slash=False
        elif slash:
            game=game+msg.topic[i]
        elif not  slash and  msg.topic[i]!='/': 
            player=player+msg.topic[i]
    return [game,player,payload]

def updtTable(tableToUp,idRow,columnsToUp,datasToUp):
   key=""
   if tableToUp == "Games":
        key="IdGame"
   elif tableToUp =="IA_DB" :
        key="position"
   for i in range(0,len(datasToUp)):
       update_query="UPDATE  "+tableToUp+" SET "+columnsToUp[i]+'= "'+str(datasToUp[i])+'" WHERE '+key+'= "'+idRow+'"'
       cur.execute(update_query)

def dltRow(idRow):
    dlt_query='DELETE FROM Games WHERE IdGame="'+idRow+'"'
    cur.execute(dlt_query)
    print("Party "+idRow+" deleted from Database")
    
def addValues(tableTarget,columns,values):
    query_value=values
    maria_query="INSERT INTO "+tableTarget+" ("+columns+") VALUES("+values+")"
    try:
        cur.execute(maria_query)
        conn.commit()   
    except mariadb.Error as e:
        print(f"Error:{e}")

def writeLog(fileLog,infoLogged):
    l=open(fileLog,"a")
    now= datetime.now()
    l.write("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
    l.write(f"{now} :: {infoLogged}\n")
    l.write("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")

    l.close()

def checkTimestamp():
    listToUp=[]
    check_query='SELECT IdGame,IdJ1,IdJ2,Timestamp,IntervalTime,Status FROM Games'
    dt = round(datetime.timestamp(datetime.now()))
    cur.execute(check_query)
    for(IdGame,IdJ1,IdJ2,Timestamp,IntervalTime,Status) in cur:                   
        if Status != "FINISHING" and Timestamp and IntervalTime:  
            if dt-int(Timestamp) >= int(IntervalTime):
                publish(client,IdGame+"/"+IdJ1+"/In","G97")
                publish(client,IdGame+"/"+IdJ2+"/In","G97")
                Id=IdGame
                listToUp.append(Id)
                writeLog("logId.txt",IdJ1+" unassigned")
                writeLog("logId.txt",IdJ2+" unassigned")
    for j in range(len(listToUp)):
       # updtTable("Games",listToUp[j],["Status"],["FINISHING"])
        dltRow(listToUp[j])
        for topic in topics:
            if topic in listToUp[j]:
                topic.remove(topics.index(topic))
        print(f"Game {listToUp[j]} closed cause to TimedOut")

def run():
    st=dt
    subscribe(client)
    while 1:    
        client.loop_start()
        checkTimestamp()     
        st=datetime.now()
        client.loop_stop()

client = connect_mqtt()
dt=datetime.now()

if __name__ == '__main__':
    l=open("logId.txt","w")
    l.write(f"==================FichierLog===================\nServeur started at {dt}\n")
    l.close()
    run()
