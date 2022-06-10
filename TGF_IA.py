# python3.8

import random 
import mariadb
import sys
import time
import json
import os
from random import choice
from paho.mqtt import client as mqtt_client
from datetime import datetime
import operator
from operator import xor

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

global loop
loop=0
broker = 'vd6db0fa.eu-central-1.emqx.cloud'
port = 15775
adrMacs=["00:00:00:00:00:00"]
topics = ["TGF"]
client_id = 'TheGrumpyFrenchman'
username = 'TGF'
password = '123Soleil'
class TopicSP:
    def __init__(self,sub,pub,game,player):
        self.sub=sub
        self.pub=pub
        self.game=game
        self.player=player


def border(prvMoves):
    firstX=-1
    firstY=-1
    lastX=0
    lastY=0
    border1=[]
    border2=[] 
    for m in reversed(prvMoves):             
        if firstX==-1 and firstY==-1 and (m[0]=='H'or m[0]=='S'):
            firstX=int(m[1])
            firstY=int(m[2])
            lastX=firstX
            lastY=firstY
        elif m[0]=='H' :
            if abs(firstX-lastX)<abs(firstX-int(m[1])):
                lastX=int(m[1])
            if abs(firstY-lastY)<abs(firstY-int(m[2])):
                lastY=int(m[2])
        elif m[0]=='S':
            break
    print([lastX,lastY])
    print([firstX,firstY])
    if firstX==lastX:
        if firstY > lastY:
            border1=[firstX,firstY+1]
            border2=[lastX,lastY-1]
        elif lastY > firstY:
            border1=[firstX,firstY-1]
            border2=[lastX,lastY+1]
    elif firstY==lastY:
        if firstX > lastX:
            border1=[firstX+1,firstY]
            border2=[lastX-1,lastY]
        elif lastX > firstX:
            border1=[firstX-1,firstY]
            border2=[lastX+1,lastY]
    
    return [border1,border2]



class DataDB:
    def __init__(self,lvl,boats,leftBoats,deadPos,currentPattern,previousMoves,remainingBoats,lastMove):
        self.lvl=lvl
        self.boats=boats
        self.leftBoats=leftBoats
        self.deadPos=deadPos
        self.currentPattern=currentPattern
        self.lastMove=lastMove
        self.remainingBoats=remainingBoats
        self.previousMoves=previousMoves
    def insertValue(self,key,valueKey):
        if key=="lvl":
            self.lvl=valueKey
        elif key=="boats":
            self.boats=valueKey
        elif key=="leftBoats":
            self.leftBoats=valueKey
        elif key=="deadPos":
            self.deadPos=valueKey
        elif key=="currentPattern":
            self.currentPattern=valueKey
        elif key=="previousMoves":
            self.previousMoves=valueKey
        elif key=="remainingBoats":
            self.remainingBoats=valueKey
        elif key=="lastMove":
            self.lastMove=valueKey

    def updtAtk(self,pos):
        x=int(pos[1])
        y=int(pos[2])
        prtMsg="Coulé"
        os.system('clear')
        if pos[0] in "HS":
            for i in range(-1,2):
                for j in range(-1,2):  
                    if i!=0 and j!=0 and x+i>=0 and  x+i<10 and  y+j>=0 and y+j<10 and [x+i,y+j] not in self.deadPos: 
                        self.deadPos.append([x+i,y+j])
            prtMsg="Touché"
            if self.currentPattern == "searchBoat":
                self.currentPattern="searchAdj"
            elif self.currentPattern == "searchAdj" and pos[0] == 'H':
                self.currentPattern="searchSunk"
            elif (self.currentPattern=="searchSunk" or self.currentPattern=="searchAdj") and pos[0] =='S':
                   
                prtMsg="Touché Coulé"
                self.currentPattern="searchBoat"
        prtMsg=prtMsg+" en position "+pos[1:3]+" !"
        print(prtMsg)
        self.deadPos.append([x,y])     
        self.previousMoves.pop()
        self.previousMoves.append(pos)
        self.lastMove=pos
        grillAtk(self.previousMoves)
        if pos[0] =='S':
            brdr=border(self.previousMoves)
            print(brdr)
            for b in brdr:
                if b not in self.deadPos and b[1]>=0 and b[1]<10 and b[0]>=0 and b[0]<10:
                    self.deadPos.append(b)
        grillDead(self.deadPos)
        return self

    def checkAtk(self,pos):
        x=int(pos[1])
        y=int(pos[2])
        posTarget=[x,y]
        tempFleet=[]
        tempBoat=[]
        answer="M"
        for boat in self.leftBoats:    
            if posTarget not in boat:
                tempFleet.append(boat)
            else :
                boat.remove(posTarget)
                if boat: 
                    tempFleet.append(boat)
                    answer='H'
                else:
                    answer='S'
        self.leftBoats=tempFleet
        answer=str(answer+pos[1:3])
        return [answer,self]

    def atk(self):
        target=0
        def readDeadPos(deadPos):
            modDP=[]
            pos=""
            for d in deadPos:
                pos=int(str(d[0])+str(d[1]))
                modDP.append(pos)
            return modDP

        def calcAdj(x,y):
            adj=[]
            for i in range(-1,2):
                for j in range(-1,2):
                    if xor(j==0,i==0) and x+i<10 and y+j<10 and x+i>=0 and y+j>=0:
                        adj.append([x+i,y+j])
            return adj

        def searchBoat(self):
            dz = readDeadPos(self.deadPos)
            if self.lvl==1:
                i=choice([p for p in range(0,99) if p not in dz])        
                target=""
                if i < 10:  
                    target="0"+str(i)
                else:
                    target=str(i)
            return target

        def searchAdj(self):
            if self.lvl==1:       
                for m in reversed(self.previousMoves):
                    if m[0]=='H':
                        lastHit=m
                        print(m)
                        break
                x=int(lastHit[1])
                y=int(lastHit[2])  
                target=""
                adj=calcAdj(x,y)
                for d in self.deadPos:
                    if d in adj:
                        adj.remove(d)
                n=random.randint(0,len(adj)-1)
                target=str(adj[n][0])+str(adj[n][1])                 
            return target

        def searchSunk(self):
            evenTarget=border(self.previousMoves) 
            for e in evenTarget:
                if e in self.deadPos or e[1]<0 or e[1]>9 or e[0]<0 or e[0]>9:
                    evenTarget.remove(e)
            if self.lvl==1:
                n=random.randint(0,len(evenTarget)-1)
                target=str(evenTarget[n][0])+str(evenTarget[n][1])
            return target
           
        if self.currentPattern=="searchBoat":
            target=searchBoat(self)
        elif self.currentPattern=="searchAdj":
            target=searchAdj(self)
        elif self.currentPattern=="searchSunk":
            target=searchSunk(self) 
        self.lastMove="T"+target
        self.previousMoves.append("T"+target)       
        return ["T"+target,self]

def newMac(lastMac):
    newAdress="" 
    incremented=False
    if lastMac:
        for c in reversed(lastMac):
            if c !=':' and int(c)+1<9 and not incremented:
                newAdress=str(int(c)+1)+newAdress
                incremented=True
            elif c!=':' and int(c)+1>9 and not incremented:
                newAdress="0"+newAdress
            elif incremented:
                newAdress=c+newAdress
    else:
        newAdress="00:00:00:00:00:00"
    
    return newAdress

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
             False
             #print(f"Send `{msgUp}` to topic `{topicUp}`")
         else:
             print(f"Failed to send message to topic {topicUp}")
         
def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg): 
        print(adrMacs,msg.topic)
        #print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        if msg.topic == "TGF" and msg.payload.decode()=="G08":
            adrMacs.append(newMac(adrMacs[-1]))
            publish(client,"newplayer",adrMacs[-1])  
        elif msg.topic in adrMacs:
            TSP=slicerTopic(msg.payload.decode())
            topics.append(TSP.sub)
            publish(client,TSP.pub,"G01") 
            time.sleep(1) 
            publish(client,TSP.pub,"G02")
            print(topics)
            ## maria_query='SELECT IdGame,IdIA,Data FROM TGF WHERE IdGame="'+TSP.game+'"'
            ## cur.execute(maria_query)
            ## for (IdGame,IdIA,Data) in cur:         
            ##      print(slicerData(Data))
        elif msg.topic in topics :
            TSP=slicerMessage(msg)
            maria_query='SELECT IdGame,IdIA,Data FROM TGF WHERE IdIA="'+TSP.player+'"'
            cur.execute(maria_query)
            sliced_data={}
            for (IdGame,IdIA,Data) in cur: 
               sliced_data=slicerData(Data)
            if msg.payload.decode()=="G03":
                gG=generateGrille()
                DataIA=DataDB(1,gG,gG,[],"searchBoat",[],[2,3,3,4,5],0)
                if sliced_data: 
                     updtTable(TSP.player,["Data"],[json.dumps(DataIA.__dict__)])
                else:
                     addValues("TGF","IdGame,IdIA,Data","'"+TSP.game+"','"+TSP.player+"','"+json.dumps(DataIA.__dict__)+"'")
                publish(client,TSP.pub,"G00")
            elif msg.payload.decode()=="G04":
                package=sliced_data.atk()
                updtTable(TSP.player,["Data"],[json.dumps(package[1].__dict__)])
                publish(client,TSP.pub,package[0])
            elif msg.payload.decode()=="G22":
                publish(client,TSP.pub,"G23")
            elif msg.payload.decode()[0] == 'T':
                package=sliced_data.checkAtk(msg.payload.decode())
                updtTable(TSP.player,["Data"],[json.dumps(package[1].__dict__)])
                if not package[1].leftBoats:
                    publish(client,TSP.pub,"G24")
                    print("Flotte coulée !!!")
                    time.sleep(1)
                publish(client,TSP.pub,package[0])
            elif msg.payload.decode()[0] in "SMH": 
                package=sliced_data.updtAtk(msg.payload.decode())
                updtTable(TSP.player,["Data"],[json.dumps(package.__dict__)])
            elif sliced_data:
                payload=msg.payload.decode()    
                sliced_data.previousMoves.append(payload)
                updtTable(TSP.player,["Data"],[json.dumps(sliced_data.__dict__)])
                if payload =="G24":
                   print("Flotte adversaire coulée")
                elif payload =="G25":
                   print("Abandon de l'adversaire")
                elif payload =="G98":
                    if TSP.player[1] == '1':
                        print("Vous avez gagné J1")
                    else:
                        print("Vous avez perdu J2")
                    publish(client,TSP.pub,"G06")
                elif payload == "G99":
                    if TSP.player[1] == '2':
                        print("Vous avez gagné J2")
                    else:
                        print("VOus avez perdu J1")
                    publish(client,TSP.pub,"G06")

        subscribe(client)
    
    for mac in adrMacs:
        client.subscribe(mac)
    for topic in topics:
        client.subscribe(topic)
        ##print(f"Subscribed to `{topic}`")
    client.on_message = on_message

def dateNow():
    return datetime.timestamp(datetime.now())

def checkFormMsg(msg):
    lnMsg=len(msg)  
    if lnMsg == 3 and mod!="np":
        if msg[0] in "HGSMT" and int(msg[1:3])>=0 and int(msg[1:3])<=99:
                    return True     
        else:
            return False

def slicerTopic(topic):
    player=""
    game=""
    slash=True
    for i in range(0,len(topic)):      

        if topic[i] == '/' and slash:
            slash=False
        elif slash:
            game=game+topic[i]
        elif not slash and topic[i]!='/':
            player=player+topic[i]
        else:
            break
    return TopicSP(f"{game}/{player}/In",f"{game}/{player}/Out",game,player)
   

def grille():
    arrayGrill=[]
    for y in range(0,10):
        for x in range(0,10):
            arrayGrill.append(f"{x}{y}")
    return arrayGrill

def generateGrille():
    arrayBoat = []
    filter5 = []
    filter4 = []
    filter3 = []
    filter2 = []
    for k in range(0,2):
        for y in range(0,10):
            for x in range (0,8):
                if k==0:
                    arrayBoat.append([[y,x],[y,x+1]])
                else:
                    arrayBoat.append([[x,y],[x+1,y]])
            for x in range(0,7):
                if k==0:
                    arrayBoat.append([[y,x],[y,x+1],[y,x+2]])
                else:
                    arrayBoat.append([[x,y],[x+1,y],[x+2,y]])
            for x in range(0,6):
                if k==0:
                    arrayBoat.append([[y,x],[y,x+1],[y,x+2],[y,x+3]])
                else:
                    arrayBoat.append([[x,y],[x+1,y],[x+2,y],[x+3,y]])
            for x in range(0,5):
                if k==0:
                    arrayBoat.append([[y,x],[y,x+1],[y,x+2],[y,x+3],[y,x+4]])
                else:
                    arrayBoat.append([[x,y],[x+1,y],[x+2,y],[x+3,y],[x+4,y]])
    for boat in arrayBoat:
        if len(boat)==5:
            filter5.append(boat)
        elif len(boat)==4:
            filter4.append(boat)
        elif len(boat)==3:
            filter3.append(boat)
        elif len(boat) == 2:
            filter2.append(boat)
    
    def updateDeadZone(combinaison,dz):
        for comb in combinaison: 
            for pos in comb:
                for i in range(-1,2):
                    for j in range(-1,2):
                        if [pos[0]+i,pos[1]+j] not in dz and (pos[0]+i)<10 and (pos[1]+j)<10 and (pos[0]+i)>=0 and (pos[1]+j)>=0:
                            dz.append([pos[0]+i,pos[1]+j])
        return dz

    def filterDeadZone(filtered,dz):
        excluded=[]    
        for posDead in dz:  
            for f in filtered:
                if posDead in f and filtered.index(f) not in excluded:
                    excluded.append(filtered.index(f))
        return filtered[choice([p for p in range(0,len(filtered)-1) if p not in excluded])]
    
    def graphicGrill(combi):
        arrayPos=[]
        for comb in combi:
            for pos in comb:
                arrayPos.append(pos)
        newLine=""
        for x in range(0,10):
            print("=====================")
            for y in range(0,10):       
                    if [x,y] in arrayPos:
                        newLine=newLine+"|#"
                    else:
                        newLine=newLine+"| "
            newLine=newLine+"|"
            print(newLine)
            newLine=""
        print("=====================")

    def newCombinaison():
        newComb=[]
        deadZone=[]
        newComb.append(filter2[random.randint(0,len(filter2)-1)])
        for i in range(0,2):
            deadZone=updateDeadZone(newComb,deadZone)
            newComb.append(filterDeadZone(filter3,deadZone))
        deadZone=updateDeadZone(newComb,deadZone)
        newComb.append(filterDeadZone(filter4,deadZone))
        deadZone=updateDeadZone(newComb,deadZone)
        newComb.append(filterDeadZone(filter5,deadZone))

        graphicGrill(newComb)
        return newComb
               
    
    return newCombinaison()

def grillAtk(lsMoves):
    arrayPos=[]
    for mv in lsMoves:
            arrayPos.append([int(mv[1]),int(mv[2])])
    newLine=""
    for x in range(0,10):
        print("=====================")
        for y in range(0,10):       
                if [x,y] in arrayPos:
                    idx=arrayPos.index([x,y])
                    if lsMoves[idx][0]=='M' :
                        newLine=newLine+"|o"
                    elif lsMoves[idx][0] in ['H','S']:
                        newLine=newLine+"|X"
                    else :
                        newLine=newLine+"| "
                else:
                    newLine=newLine+"| "
        newLine=newLine+"|"
        print(newLine)
        newLine=""
    print("=====================")


def grillDead(lsDead):
    newLine=""
    for x in range(0,10):
        print("=====================")
        for y in range(0,10):       
                if [x,y] in lsDead:
                    newLine=newLine+"|+"
                else:
                    newLine=newLine+"| "
        newLine=newLine+"|"
        print(newLine)
        newLine=""
    print("=====================")



def slicerMessage(msg):
    player=""
    game=""
    slash=True
    payload=msg.payload.decode()
    for i in range(0,len(msg.topic)):
        if msg.topic[i] =='/' and slash:
            slash=False
        elif slash:
            game=game+msg.topic[i]
        elif not  slash and  msg.topic[i]!='/': 
            player=player+msg.topic[i]
        else : 
            break
    return TopicSP(game+"/"+player+"/In",game+"/"+player+"/Out",game,player)

def slicerData(data):
    keyBool=False
    valueKeyBool=False
    key=""
    valueKey=""
    waitChars=0
    array1=[]
    array2=[]
    array3=[]
    num="1234567890"
    alpha="AZERTYUIOPQSDFGHJKLMWXCVBNazertyuiopqsdfghjklmwxcvbn"
    classedData=DataDB("","","","","","","","")
    for c in data:
        if c=='"' and not keyBool and not valueKeyBool:
            keyBool = True
        elif keyBool and not valueKeyBool and c in alpha:
            key=key+c
        elif c=='"' and keyBool and not valueKeyBool:
            valueKeyBool = True
        elif keyBool and valueKeyBool and c in alpha:
            valueKey=valueKey+c
        elif keyBool and valueKeyBool and c in num:
            if valueKey:
                if str(valueKey)[0] in alpha:
                    valueKey=str(valueKey)+c
                else:
                    valueKey=int(str(valueKey)+c)
            else: 
                valueKey=int(c)
        elif (keyBool and valueKeyBool and c ==',' and waitChars==0) or data.index(c)==len(data)-1:   
            keyBool = False
            valueKeyBool = False
            if array3 :
                valueKey=array3
            elif array2:
                valueKey=array2
            elif array1 :
                valueKey=array1
            array1=[]
            array2=[]
            array3=[]
            classedData.insertValue(key,valueKey)
            key=""
            valueKey=""
        elif keyBool and valueKeyBool and c=="," and waitChars>0 :    
            if waitChars == 3:
                array1.append(valueKey)
                valueKey=""
            elif waitChars==2:
                if array1:
                    array2.append(array1)
                    array1=[]
                else:
                    array2.append(valueKey)
                    valueKey=""
            else:
                if array2:
                    array3.append(array2)
                    array2=[]
                else:
                    array3.append(valueKey)
                    valueKey=""
        elif keyBool and valueKeyBool and c in ['[','"']:
            waitChars=waitChars+1
            if (waitChars==1 or waitChars==2) and c=='"':
                waitChars=waitChars-1
        elif keyBool and valueKeyBool and c in [']']:
            waitChars=waitChars-1
            if waitChars==2:
                array1.append(valueKey)
                valueKey=""
            elif waitChars==1:
                if array1:
                    array2.append(array1)
                    array1=[]
                else:
                    array2.append(valueKey)
                    valueKey=""
            else :
                if array2:
                    array3.append(array2)
                    array2=[]
                else:
                    if valueKey != "":
                        array3.append(valueKey)
                        valueKey=""
                    else:
                        valueKey=[]

    #print(json.dumps(classedData.__dict__))
        
    return classedData

def updtTable(idRow,columnsToUp,datasToUp):
   key="IdIA"
   for i in range(0,len(datasToUp)):
       update_query="UPDATE TGF SET "+columnsToUp[i]+"= '"+str(datasToUp[i])+"' WHERE "+key+'= "'+idRow+'"'
     
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

def run():
    st=dt
    subscribe(client)
    while 1:
        client.loop_start()
        st=datetime.now()
        client.loop_stop()
client=connect_mqtt()
dt=datetime.now()


if __name__ == '__main__':
    run()
