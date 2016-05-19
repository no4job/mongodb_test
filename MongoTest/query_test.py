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


def test_1(idList,save,projection={}):
    time_total=Timer()
    time_total.start()
    t = Timer()
    t.start()
    #****************
    documentList=[]
    for id in range(len(idList)):
        if projection:
            doc=model.find_one({"_id": idList[id]},projection)
        else:
            doc=model.find_one({"_id": idList[id]})

        # doc=model.find_one({"_id": idList[id]})
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
    documents=getAllDocuments(db,collectionName,save)
    time_total.stop()
    print("Total:{}, load time:{}".format(len(documents),time_total.elapsed))
def test_3(db,collectionName,search_doc,projection={},save=0,limit=0):
    time_total=Timer()
    time_total.start()
    #****************
    c= db[collectionName]
    #search_doc={}
    #search_doc["f1xxxxxxxx"]= search_str
    #cursor=c.find(search_doc,projection)
    if projection:
        cursor=c.find(search_doc,projection).limit(limit)
    else:
        cursor=c.find(search_doc).limit(limit)
    #cursor=c.find()
    documentList=[]
    if save:
        documentList=list(cursor)
    #documents=getAllDocuments(db,collectionName,1)
    time_total.stop()
    print("Total:{}, search time:{}".format(cursor.count(with_limit_and_skip=True),
                                            time_total.elapsed))
    # if limit:
    #     print("Total:{}, search time:{}".format(limit,time_total.elapsed))
    # else:
    #     print("Total:{}, search time:{}".format(cursor.count(),time_total.elapsed))
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
# test_1(idList,1,{"native_id":1})

#******test_1_2 success************
#load documents (54885) one by one, ,create list of objects
#Result:myPC:53.67317396700944
# idList=getObjectIDList(db,"model")
# test_1(idList,1)

#******test_2_1 success************
#find all all document in collection(54885),iterate cursor,create list of objects
#Result:myPC:78.45585345958517
# test_2(db,"model",1)
# exit(0)

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
# test_3(db,"model",{"data_section_2.f1xxxxxxxxxxxx.f2xxxxxxxx" : { "$regex": ".*1"}},
#        {"type":1})
#******test_4_1 search 1,10,100,1000 values with/without index ************
# no index
# Total:1, search time:50.26419795567872
# Total:10, search time:49.5060912571009
# Total:100, search time:48.867994727900026
# Total:1000, search time:49.72399320095735
#*******************
# Total:1, search time:50.83981541505498
# Total:10, search time:52.61573542107672
# Total:100, search time:55.38050559622964
# Total:1000, search time:53.83950319060361
# cursor=model.find({"native_id" : 10000},{})
# pprint (cursor)
# exit (0)
save=1
projection={"native_id":1}
values_1 = [1000]
values_10 = [1000+i*1000 for i in range(10)]
values_100 = [1000+i*100 for i in range(100)]
values_1000 = [1000+i*50 for i in range(1000)]
values_10000 = [1000+i*5 for i in range(10000)]
values_50000 = [1000+i*1 for i in range(50000)]
values_54885 = [1+i*1 for i in range(54886)]
values_18439 = [1+i*1 for i in range(18439)]
values_7841 = [1+i*1 for i in range(7841)]
# test_3(db,"model",{"native_id" : {"$in":values_1}},projection,save)
# test_3(db,"model",{"native_id" : {"$in":values_10}},projection,save)
# test_3(db,"model",{"native_id" : {"$in":values_100}},projection,save)
# result= test_3(db,"model",{"native_id" : {"$in":values_1000}},projection,save)
# test_3(db,"model",{"native_id" : {"$in":values_10000}},projection,save)
# test_3(db,"model",{"native_id" : {"$in":values_50000}},projection,save)
# test_3(db,"model",{"native_id" : {"$in":values_54885}},projection,save)
# result=test_3(db,"model",{},projection,1)
# result=test_3(db,"model",{"parent_object": { "$regex": "([^0]0){1,2}"}},projection,save)
# test_3(db,"model",{"native_id" : {"$in":values_18439}},projection,save)

# test_3(db,"model",{"$where" : "this.native_id % 7 == 3"},projection,save)
# test_3(db,"model",{"native_id" : {"$in":values_7841}},projection,save)
exit (0)

#test_3(db,"model",{"data_section_2" : { "$exists": True}})
#test_3(db,"model",{"data_section_2.f1xxxxxxxxxxxx" : { "$exists": True}})
#test_3(db,"model",{"data_section_2.f1xxxxxxxxxxxx.f1xxxxxxxx" : { "$exists": True}})
#test_3(db,"model",{"data_section_2" : {"f1xxxxxxxxxxxx":{}}})