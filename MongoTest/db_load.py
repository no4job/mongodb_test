__author__ = 'mdu'
import csv
from  ClusterDescriptor import *
from datetime import datetime
from pymongo import MongoClient
from pymongo import errors
from Timer import *

def createElement(clusterDescriptor,number,type,count=1,array=0):
    modelElement={}
    modelElement["name"]=clusterDescriptor.get_s1_name(number)
    modelElement["id"]=clusterDescriptor.get_s1_name(number)
    modelElement["type"]=clusterDescriptor.get_s1_type(type)
    modelElement["description"]=clusterDescriptor.get_s1_description(number)
    modelElement["native_id"]=""
    modelElement["parent_object"]=""
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
time_total=Timer()
time_total.start()
t = Timer()
t.start()

with open (clusterDescriptionFile,"r") as f:
    clusterDescriptions = csv.DictReader(f,delimiter=";")
    for clusterDescriptionDict in clusterDescriptions:
        clusterDescriptor = ClusterDescriptor(**clusterDescriptionDict)
        for element_number in range(1,clusterDescriptor.document_number+1):
            #for element_type in range(1,clusterDescriptor.types_number+1):
            element_type = element_number %  clusterDescriptor.types_number
            element_type = element_type if element_type else 1
            document=createElement(clusterDescriptor,element_number,element_type,element_number)
            try:
                model.insert(document)
            except errors.InvalidDocument as err:
                print(str(err))
            if element_number % 1000  == 0 or element_number==1:
                t.stop()
                print(t.elapsed)
                t.reset()
                t.start()
                print("cluster:"+str(clusterDescriptor.cluster)+"   element:"+str(element_number))
t.stop()
print(t.elapsed)
time_total.stop()
print("cluster:"+str(clusterDescriptor.cluster)+"   element:"+str(element_number))
print("Total import time:{}".format(time_total.elapsed))
print("Inserted documents:{}".format(model.count()))



