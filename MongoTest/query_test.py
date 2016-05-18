__author__ = 'MishninDY'
from  ClusterDescriptor import *
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from pymongo import errors
from Timer import *
import itertools
from pprint import pprint

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
def getAllDocuments(db,collectionName,save):
    c = db[collectionName]
    cursor=c.find({})
    #cursor=itertools.islice(cursor, 0, 1)
    documentList=[]
    if save:
        documentList=list(cursor)
    # for document in idList:
    #     pprint(document)
    return documentList

def getAllDocuments_(db,collectionName):
    idList=getObjectIDList(db,"model")
    for id in range(len(idList)):
        doc=model.find_one({"_id": idList[id]})
        idList.append(doc)
    return idList


def test_1(idList,save):
    time_total=Timer()
    time_total.start()
    t = Timer()
    t.start()
    #****************
    documentList=[]
    for id in range(len(idList)):
        doc=model.find_one({"_id": idList[id]})
        if save:
            documentList.append(doc)
        if id % 10000  == 0 or idList==0:
            t.stop()
            print(str(id)+" : "+str(t.elapsed))
            t.reset()
            t.start()
    time_total.stop()
    print("Total:{}, load time:{}".format(len(idList),time_total.elapsed))
def test_2(db,collectionName,save):
    time_total=Timer()
    time_total.start()
    #****************
    documents=getAllDocuments(db,collectionName,1)
    time_total.stop()
    print("Total:{}, load time:{}".format(len(documents),time_total.elapsed))
def test_3(db,collectionName,search_doc,projection={},save=0):
    time_total=Timer()
    time_total.start()
    #****************
    c= db[collectionName]
    #search_doc={}
    #search_doc["f1xxxxxxxx"]= search_str
    cursor=c.find(search_doc,projection)
    #cursor=c.find()
    documentList=[]
    if save:
        documentList=list(cursor)
    documents=getAllDocuments(db,collectionName,1)
    time_total.stop()
    print("Total:{}, search time:{}".format(cursor.count(),time_total.elapsed))
    return documentList

# import platform
# print(platform.architecture())
client = MongoClient()
db=client.modelDB
model = db.model
#idList=getObjectIDList(db,"model")
#***failed: document size more than 16mb *******************
# all_documents={}
# all_documents["all"]=getAllDocuments(db,"model")
# model_one_doc = db.model_one_doc
# model_one_doc.insert(all_documents)

#******test_1_1 success************
#load documents (54885) one by one, objects were not saved
#Result:21.599647520728542:myPC:37.52616617560763
# idList=getObjectIDList(db,"model")
# test_1(idList,0)

#******test_1_2 success************
#load documents (54885) one by one, ,create list of objects
#Result:myPC:53.67317396700944
# idList=getObjectIDList(db,"model")
# test_1(idList,1)

#******test_2_1 success************
#find all all document in collection(54885),iterate cursor,create list of objects
#Result:myPC:78.45585345958517
#test_2(db,"model",1)

#******test_2_2 success************
#find all all document in collection(54885) only
#Result:myPC:72.84945136057661
#test_2(db,"model",0)
#************************************

#******test_3_1 success************
#search exact field value,
#Result:myPC:Search:{"type" : "1xxxxxxxxxxxxxx"},Total:20521, search time:66.50377626448982
#Result:myPC:Search:{"type" : "1xxxxyxxxxxxxxx"},Total:0, search time:62.46466947988706
#Result:myPC:Search:{"data_section_2.f1xxxxxxxxxxxx.f1xxxxxxxx" : "1xxxxxxxxxxxxxxxx"},
#                    Total:32670, search time:50.10246991431302
#Result:myPC:Search:{"data_section_2.f1xxxxxxxxxxxx.f1xxxxxxxx" : "1yxxxxxxxxxxxxxxx"},
#                    Total:0, search time:50.419373892044845
#Result:myPC:Search:{"data_section_2.f1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.f1xxxxxxxxx" : "1xxxxxxxxxxx"},
#                    Total:1172, search time:52.2118590757849
#Result:myPC:Search:{"data_section_2.f1xxxxxxxxxxxx.f2xxxxxxxx" : "2xxxxxxxxxxxxxxxx1"},
#                   Total:326, search time:50.10789870255955
#Result:myPC:Search:{"data_section_2.f1xxxxxxxxxxxx.f2xxxxxxxx" : { "$regex": ".*1"}},
#                   Total:326, search time:51.100138407985575

#test_3(db,"model",{"data_section_2" : { "$exists": True}})
#test_3(db,"model",{"data_section_2.f1xxxxxxxxxxxx" : { "$exists": True}})
#test_3(db,"model",{"data_section_2.f1xxxxxxxxxxxx.f1xxxxxxxx" : { "$exists": True}})
test_3(db,"model",{"data_section_2.f1xxxxxxxxxxxx.f2xxxxxxxx" : { "$regex": ".*1"}},
       {"type":1})
#test_3(db,"model",{"data_section_2" : {"f1xxxxxxxxxxxx":{}}})