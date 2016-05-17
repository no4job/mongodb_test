__author__ = 'MishninDY'
from  ClusterDescriptor import *
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from pymongo import errors
from Timer import *

def getObjectIDList_str(db,collectionName):
    c = db[collectionName]
    #print("Number of documents:{}".format(c.count()))
    cursor=c.find({}, {"_id" : 1})
    idList=list(map(str,(d["_id"]for d in cursor)))
    return idList
def getObjectIDList(db,collectionName):
    c = db[collectionName]
    cursor=c.find({}, {"_id" : 1})
    idList=list(d["_id"]for d in cursor)
    return idList
def getAllDocuments_(db,collectionName):
    c = db[collectionName]
    cursor=c.find({})
    #idList=list(cursor)
    # for document in cursor:
    #     idList.append(document)
    count = 0
    error = 0
    good_after_error=0
    while True:
        try:
            count+=1
            if error:
                good_after_error+=1
            document = next(cursor)
            idList.append(document)
        except StopIteration:
            break
        except:
            error+=1
    print("Total count: {} Error count {}: Good count after error{}:".format(count,error,good_after_error))
    return idList

def getAllDocuments(db,collectionName):
    idList=getObjectIDList(db,"model")
    for id in range(len(idList)):
        doc=model.find_one({"_id": idList[id]})
        idList.append(doc)
    return idList

#load documents (54885) one by one
#Result:21.599647520728542
def test_1(idList):
    time_total=Timer()
    time_total.start()
    t = Timer()
    t.start()
    #****************
    for id in range(len(idList)):
        doc=model.find_one({"_id": idList[id]})
        if id % 10000  == 0 or idList==0:
            t.stop()
            print(str(id)+" : "+str(t.elapsed))
            t.reset()
            t.start()
    time_total.stop()
    print("Total:{}, load time:{}".format(len(idList),time_total.elapsed))
import platform
print(platform.architecture())
client = MongoClient()
db=client.modelDB
model = db.model
idList=getObjectIDList(db,"model")
all_documents={}
all_documents["all"]=getAllDocuments(db,"model")
#******************
test_1(idList)
#******************

