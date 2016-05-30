__author__ = 'mdu'
import csv
from  ClusterDescriptor import *
from datetime import datetime
from pymongo import MongoClient
from pymongo import errors
#from pymongo import IndexModel, ASCENDING, DESCENDING
from Timer import *

def getDocumentSize(document):
    documentSize=0
    if type(document) == dict:
        for field in document:
            documentSize+= len(field)
            documentSize+= getDocumentSize(document[field])
        return documentSize
    elif type(document) == list:
        for field in document:
            documentSize+= getDocumentSize(field)
        return documentSize
    else:
        return len(str(document))


def createElement(clusterDescriptor,number,type,count=1,array=0):
    modelElement={}
    modelElement["name"]=clusterDescriptor.get_s1_name(number)
    modelElement["id"]=clusterDescriptor.get_s1_name(number)
    modelElement["type"]=clusterDescriptor.get_s1_type(type)
    modelElement["description"]=clusterDescriptor.get_s1_description(number)
    modelElement["native_id"]=count
    modelElement["parent_object"]="x"+str(count)
    #*************************
    modelElement["model_test_dict"]={}
    modelElement["model_test_dict"]["list_of_values"]=[1,2,3,4,5,6,7,8,9,10]
    modelElement["model_test_dict"]["list_of_dicts"]=[
                                                        {"value_1":11,"value_2":12,"value_3":13},
                                                        {"value_1":21,"value_2":22,"value_3":23},
                                                        {"value_1":31,"value_2":32,"value_3":33},
                                                        {"value_1":41,"value_2":42,"value_3":43},
                                                        {"value_1":51,"value_2":52,"value_3":53},
                                                        {"value_1":61,"value_2":62,"value_3":63},
                                                        {"value_1":71,"value_2":72,"value_3":73},
                                                        {"value_1":81,"value_2":82,"value_3":83},
                                                        {"value_1":91,"value_2":92,"value_3":93},
                                                        {"value_1":101,"value_2":102,"value_3":103}
                                                      ]
    modelElement["model_test_dict"]["value"]=1
    modelElement["model_test_array"]=[1,2,3,4,5,6,7,8,9,10]
    #***************************
    modelElement["model_revision"]=""
    modelElement["creation_date"] = str(datetime.now()).replace('.', '\uff0E')
    modelElement["change_date"]= str(datetime.now()).replace('.', '\uff0E')
    modelElement["archive"]="false"
    modelElement["deleted"]="false"
    data_section_2={}
    for field_x_number in range(1,clusterDescriptor.field_x_number+1):
        if array:
            field_x=[]
        else:
            field_x={}
        field_x_name=clusterDescriptor.get_field_x_name(field_x_number)
        data_section_2[field_x_name]=field_x
        if array:
            data_section_2[field_x_name].append({})
        if field_x_number > clusterDescriptor.field_x_number*clusterDescriptor.empty_field_x_ratio/100:
            continue
        for field_x_x_number in range(1,clusterDescriptor.field_x_x_number+1):
                field_x_x_name=clusterDescriptor.get_field_x_x_name(field_x_x_number)
                field_x_x_value=clusterDescriptor.get_field_x_x_value(field_x_x_number)
                #**********************
                if field_x_x_number == 2 and count % 100 ==0:
                    field_x_x_value = field_x_x_value+"1"
                #**********************
                if array:
                    data_section_2[field_x_name][0][field_x_x_name]=field_x_x_value
                else:
                    data_section_2[field_x_name][field_x_x_name]=field_x_x_value
    modelElement["data_section_2"]=data_section_2
    return modelElement


clusterDescriptionFile="cluster_description.csv"
client = MongoClient()
client.drop_database("modelDB")
db=client.modelDB
model = db.model
#model.create_index("native_id")
model_idx = db.model_idx
model_idx.create_index("native_id")
time_total=Timer()
time_total.start()
t = Timer()
t.start()
db_t = Timer()


count=0
element_size={}
bulk = db.model.initialize_unordered_bulk_op()
bulk_idx = db.model_idx.initialize_unordered_bulk_op()
with open (clusterDescriptionFile,"r") as f:
    clusterDescriptions = csv.DictReader(f,delimiter=";")
    for clusterDescriptionDict in clusterDescriptions:
        clusterDescriptor = ClusterDescriptor(**clusterDescriptionDict)
        #*******************
        document=createElement(clusterDescriptor,clusterDescriptor.document_number+1,1,count+1+clusterDescriptor.document_number)
        max=getDocumentSize(document)
        document=createElement(clusterDescriptor,1,1,count+1)
        min=getDocumentSize(document)
        element_size[clusterDescriptor.cluster]= []
        element_size[clusterDescriptor.cluster].append((max+min)/2)
        element_size[clusterDescriptor.cluster].append((max-min)/element_size[clusterDescriptor.cluster][0])
        #*******************
        for element_number in range(1,clusterDescriptor.document_number+1):
            count+=1
            #for element_type in range(1,clusterDescriptor.types_number+1):
            element_type = element_number %  clusterDescriptor.types_number
            element_type = element_type if element_type else 1
            document=createElement(clusterDescriptor,element_number,element_type,
                                   count)
            try:
                db_t.start()
                #model.insert(document)
                bulk.insert(document)
                bulk_idx.insert(document)
                if count % 990 == 0:
                    bulk.execute()
                    bulk = db.model.initialize_unordered_bulk_op()
                    bulk_idx.execute()
                    bulk_idx = db.model_idx.initialize_unordered_bulk_op()
                db_t.stop()
            except errors.InvalidDocument as err:
                print(str(err))
            if element_number % 1000  == 0 or element_number==1:
                t.stop()
                print(t.elapsed)
                t.reset()
                t.start()
                print("cluster:"+str(clusterDescriptor.cluster)+"   element:"+str(element_number))
try:
    bulk.execute()
    bulk_idx.execute()
except errors.InvalidOperation as io:
    pass

t.stop()
print(t.elapsed)
time_total.stop()
print("cluster:"+str(clusterDescriptor.cluster)+"   element:"+str(element_number))
print("Total import time:{}".format(time_total.elapsed))
print("DB insert time:{}".format(db_t.elapsed))
print("Inserted documents:{}".format(model.count()))
for k,v in element_size.items():
    print("cluster {} element size: {}, +/-{:.2%}".format(k, v[0],v[1]))




