# coding: utf8
import copy

__author__ = 'mdu'

from pymongo import MongoClient
from pymongo import errors
import datetime
try:
    from lxml import etree
except ImportError:
    print("lxml import error")
    raise
from datetime import datetime
import sys
sys.path.append("C:\IdeaProjects\tomita_tools\MongoTest")
from Timer import *
import json
import zipfile
#import uuid
import os
global_element_id=0
global_attribute_id=0
global_attribute_record_id=0

def get_model_element(elem):
    global global_attribute_id
    global global_element_id
    global global_attribute_record_id
    global_element_id+=1
    modelElement={}
    #*****************************
    modelElement["parent_object"]=""
    modelElement["model_revision"]=""
    modelElement["description"]=""
    modelElement["native_id"]=""
    modelElement["creation_date"]=str(datetime.now())
    modelElement["change_date"]=str(datetime.now())
    modelElement["archive"]="false"
    modelElement["deleted"]="false"
    #***************************************************
    modelElementSize=0
    #***************************************************

    modelElement["name"]=next(iter(elem.xpath("./@ElementName")),"")
    modelElement["id"]=next(iter(elem.xpath("./@ElementId")),"")
    modelElement["type"]=next(iter(elem.xpath("./@TypeID")),"")
    modelElement["symbol"]=next(iter(elem.xpath("./@ElementSymbol")),"")
    modelElement["ElementUUID"]=str(global_element_id)
    data_section_2=[]
    record_counter=0
    #value_counter=0
    value_sections_counter=0
    value_size_counter=0
    attribute_counter=0
    field_stat=[]
    attribute_stat=[]
    #*****************************************************
    #***model size addition per one element
    modelElementSize=modelElementSize+ \
                     len(modelElement["name"])+len(modelElement["id"])+ \
                     len(modelElement["type"])+len(modelElement["symbol"])
    #*****************************************************
    for attributeElement in elem.xpath("./www.qpr.com:Attribute",namespaces={'www.qpr.com': 'www.qpr.com'}):
        global_attribute_id+=1
        AttributeSize=0
        Attribute_field_NameSize=0
        Attribute_field_ValueSize=0
        #***********************************
        attr_record_counter=0
        attr_value_counter=0
        attr_value_size_counter=0
        #*************************************
        attribute_counter+=1
        fieldName=next(iter(attributeElement.xpath("./@AttributeName")),"")
        fieldName=fieldName.replace('.', '\uff0E')
        field={}
        field_values_array=[]
        valueArray=[]
        valueArray.extend(attributeElement.xpath("./www.qpr.com:Value/text()",namespaces={'www.qpr.com': 'www.qpr.com'}))

        if len(valueArray):
            field_values_array.append(dict(Value=valueArray))
            value_sections_counter+=1
            #value_counter+=len(valueArray)
            value_size_counter+=sum(len(str(v))for v in valueArray)
            attr_value_counter+=len(valueArray)
            attr_value_size_counter+=sum(len(str(v))for v in valueArray)
        records=[]
        for recordElement in attributeElement.xpath("./www.qpr.com:Record",namespaces={'www.qpr.com': 'www.qpr.com'}):
            global_attribute_record_id+=1
            #record=[]
            record={}
            recordsFieldElement=recordElement.xpath("./www.qpr.com:Field",namespaces={'www.qpr.com': 'www.qpr.com'})
            attr_record_counter=len(recordsFieldElement)
            for recordFieldElement in recordsFieldElement:
                recordFieldName=next(iter(recordFieldElement.xpath("./@Name")),"")
                recordFieldName=recordFieldName.replace('.', '\uff0E')
                recordFieldValue=next(iter(recordFieldElement.xpath("./@Value")),"")
                #recordField={}
                #recordField[recordFieldName]=recordFieldValue
                #record.append(recordField)
                record[recordFieldName]=recordFieldValue
                record_counter+=1
                one_field_stat={}
                one_field_stat["field_name_size"]=len(str(recordFieldName))
                one_field_stat["field_value_size"]=len(str(recordFieldValue))
                one_field_stat["AttributeName_size"]=len(str(fieldName))
                one_field_stat["attr_value_counter"]=attr_value_counter
                one_field_stat["attr_value_size_counter"]=attr_value_size_counter
                one_field_stat["attr_record_counter"]=attr_record_counter

                #one_field_stat["AttributeUUID"]=uuid.uuid4()
                #one_field_stat["ElementUUID"]=uuid.uuid4()
                one_field_stat["RecordUUID"]=  global_attribute_record_id
                one_field_stat["AttributeUUID"]=  global_attribute_id
                one_field_stat["ElementUUID"]= global_element_id


                field_stat.append(one_field_stat)
                #*****************************************************
                #***model size addition per one field_NameValue
                modelElementSize=modelElementSize+ \
                                 one_field_stat["field_name_size"]+one_field_stat["field_value_size"]
                AttributeSize=AttributeSize+ \
                              one_field_stat["field_name_size"]+one_field_stat["field_value_size"]
                Attribute_field_NameSize+= one_field_stat["field_name_size"]
                Attribute_field_ValueSize+= one_field_stat["field_value_size"]

                #*****************************************************
            records.append(record)
        if len(records):
            field_values_array.append(dict(Record=records))
        else:
            one_field_stat={}
            one_field_stat["field_name_size"]=0
            one_field_stat["field_value_size"]=0
            one_field_stat["AttributeName_size"]=len(str(fieldName))
            one_field_stat["attr_value_counter"]=attr_value_counter
            one_field_stat["attr_value_size_counter"]=attr_value_size_counter
            one_field_stat["attr_record_counter"]=0
            #one_field_stat["AttributeUUID"]=uuid.uuid4()
            #one_field_stat["ElementUUID"]=uuid.uuid4()
            one_field_stat["RecordUUID"]=  global_attribute_record_id
            one_field_stat["AttributeUUID"]=  global_attribute_id
            one_field_stat["ElementUUID"]= global_element_id
            field_stat.append(one_field_stat)
            #records.append(record)

        #*****************************************************
        #***model size addition per one attribute
        modelElementSize=modelElementSize+\
                        attr_value_size_counter+\
                        len(str(fieldName))
        AttributeSize=AttributeSize+ \
                         attr_value_size_counter+ \
                         len(str(fieldName))
        one_attribute_stat={}
        one_attribute_stat["AttributeSize"]= AttributeSize
        one_attribute_stat["AttributeName_size"]=one_field_stat["AttributeName_size"]
        one_attribute_stat["attr_value_counter"]=one_field_stat["attr_value_counter"]
        one_attribute_stat["attr_value_size_counter"]=one_field_stat["attr_value_size_counter"]
        one_attribute_stat["attr_record_counter"]=one_field_stat["attr_record_counter"]
        #one_attribute_stat["RecordUUID"]=one_field_stat["RecordUUID"]
        one_attribute_stat["attr_record_counter"]=one_field_stat["attr_record_counter"]
        one_attribute_stat["Attribute_field_NameSize"]=Attribute_field_NameSize
        one_attribute_stat["Attribute_field_ValueSize"]=Attribute_field_ValueSize
        one_attribute_stat["AttributeUUID"]=one_field_stat["AttributeUUID"]
        one_attribute_stat["ElementUUID"]=one_field_stat["ElementUUID"]
        attribute_stat.append(one_attribute_stat)
        #*****************************************************
        field[fieldName]=field_values_array
        data_section_2.append(field)

    modelElement["data_section_2"]=data_section_2
    result={}
    result["modelElement"]= modelElement
    result["record_counter"]= record_counter
    #result["value_counter"]= value_counter
    #*****************************************************
    #result["element_size"]=len(str(json.dumps(modelElement,ensure_ascii=False)))
    result["element_size"]=modelElementSize
    #*****************************************************
    result["attribute_counter"]= attribute_counter
    result["value_sections_counter"]= value_sections_counter
    result["value_size_counter"]= value_size_counter
    result["field_stat"]=field_stat
    result["attribute_stat"]=attribute_stat
    return result
def uniq(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]
def remove_dots(data):
    if type(data)is dict:
        for key in data.keys():
            if type(data[key]) is dict:
                data[key] = remove_dots(data[key])
            elif type(data[key])is list:
                for item in data[key]:
                    #remove_dots(item)
                    if type(item)is dict or type(item)is list:
                        remove_dots(item)

            if '.' in key:
                data[key.replace('.', '\uff0E')] = data[key]
                #data[key.replace('.', '#')] = data[key]
                del data[key]
    if type(data)is list:
        for item in data:
            #remove_dots(item)
            if type(item)is dict or type(item)is list:
                remove_dots(item)
                #if item is dict: data[key] = remove_dots(data[key])
    return data
if __name__ == '__main__':
    time_total=Timer()
    time_total.start()
    t = Timer()
    t.start()
    # start_time=datetime.now()
    # print("Start processing: "+start_time.strftime("%d.%m.%Y %H:%M:%S.%f"))
    # client = MongoClient()
    # client.drop_database("modelDB")
    # db=client.modelDB
    #
    # model = db.model

    #input_file='C:\\IdeaProjects\\hh_api_test\\MongoTest\\exp_types_formatted_few_elements.xml'
    #***input_file='exp_types_formatted_few_elements.xml'
    #****input_file='C:\\Users\mdu\\Documents\\qpr_export\\exp.xml'
    input_file='C:\\Users\МишинДЮ\\Documents\\qpr_export\\exp.xml'
    events = ("start", "end")
    context = etree.iterparse(input_file,events = events, tag=('{www.qpr.com}ModelElement'))
    count=0
    elements_with_dot_count=0
    #elements_with_dot_count_no_Multiplicity=0
    corrected_modelElement={}
    err_msg={}
    spr="\t"
    spr_field_stat="\t"
    head="name"+spr+"id"+spr+"type"+spr+"symbol"+spr+"element_size"+spr+"record_counter"+\
         spr+"value_sections_counter"+spr+"value_size_counter"+spr+"attribute_counter"+ \
         spr+"ElementUUID"+"\n"
    csv=head
    head="AttributeName_size"+spr_field_stat+"field_name_size"+spr_field_stat+"field_value_size"+spr_field_stat+ \
         "attr_value_counter"+spr_field_stat+"attr_value_size_counter"+spr_field_stat+"attr_record_counter"+ \
         spr_field_stat+"AttributeSize"+spr_field_stat+"element_size"+spr_field_stat+"type"+\
         spr_field_stat+"RecordUUID"+spr_field_stat+"AttributeUUID"+\
         spr_field_stat+"ElementUUID"+"\n"
    csv_field_stat=head
    head="AttributeName_size"+spr_field_stat+"attr_value_counter"+spr_field_stat+"attr_value_size_counter"+spr_field_stat+ \
         "attr_record_counter"+spr_field_stat+"AttributeSize"+spr_field_stat+\
         "Attribute_field_NameSize"+spr_field_stat+"Attribute_field_ValueSize"+spr_field_stat+ \
         "attribute_counter"+spr_field_stat+"AttributeUUID"+spr_field_stat+"ElementUUID"+"\n"
    csv_attribute_stat=head

    with open("model_stat_field_stat.csv","w+",encoding="utf-8")as model_stat_field_stat,\
         open("model_stat_attribute_stat.csv","w+",encoding="utf-8")as model_stat_attribute_stat:
        for action, elem in context:
            if action=="end":
                count+=1
                modelElement=get_model_element(elem)
                #*******************************************
                # try:
                #     model.insert(modelElement)
                # except errors.InvalidDocument as err:

                    # elements_with_dot_count+=1
                    # err_msg[modelElement["id"]]=str(err)


                sq=[]
                #sq.append(modelElement["modelElement"]["name"])
                sq.append("x"*len(modelElement["modelElement"]["name"]))
                sq.append(modelElement["modelElement"]["id"])
                sq.append(modelElement["modelElement"]["type"])
                sq.append(modelElement["modelElement"]["symbol"])
                sq.append(str(modelElement["element_size"]))
                sq.append(str(modelElement["record_counter"]))
                sq.append(str(modelElement["value_sections_counter"]))
                sq.append(str(modelElement["value_size_counter"]))
                sq.append(str(modelElement["attribute_counter"]))
                sq.append(str(modelElement["modelElement"]["ElementUUID"]))
                #sq.append(str(modelElement["value_counter"]))
                #sq_field_stat=[]
                attribute_size_data={}
                for one_field_attribute_dict in modelElement["attribute_stat"]:
                    attribute_size_data[str(one_field_attribute_dict["AttributeUUID"])]= \
                        str(one_field_attribute_dict["AttributeSize"])
                for one_field_stat_dict in modelElement["field_stat"]:
                    one_field_stat=[]
                    one_field_stat.append(str(one_field_stat_dict["AttributeName_size"]))
                    one_field_stat.append(str(one_field_stat_dict["field_name_size"]))
                    one_field_stat.append(str(one_field_stat_dict["field_value_size"]))
                    one_field_stat.append(str(one_field_stat_dict["attr_value_counter"]))
                    one_field_stat.append(str(one_field_stat_dict["attr_value_size_counter"]))
                    one_field_stat.append(str(one_field_stat_dict["attr_record_counter"]))
                    one_field_stat.append(str(attribute_size_data[str(one_field_stat_dict["AttributeUUID"])]))
                    one_field_stat.append(str(modelElement["element_size"]))
                    one_field_stat.append(modelElement["modelElement"]["type"])
                    one_field_stat.append(str(one_field_stat_dict["RecordUUID"]))
                    one_field_stat.append(str(one_field_stat_dict["AttributeUUID"]))
                    one_field_stat.append(str(one_field_stat_dict["ElementUUID"]))

                    #sq_field_stat.extend(one_field_stat)
                    csv_line_field_stat=spr_field_stat.join(one_field_stat)
                    csv_field_stat+=csv_line_field_stat+"\n"
                csv_line=spr.join(sq)
                csv+=csv_line+"\n"
                #******************************
                for one_field_attribute_dict in modelElement["attribute_stat"]:
                    one_field_attribute=[]
                    one_field_attribute.append(str(one_field_attribute_dict["AttributeName_size"]))
                    one_field_attribute.append(str(one_field_attribute_dict["attr_value_counter"]))
                    one_field_attribute.append(str(one_field_attribute_dict["attr_value_size_counter"]))
                    one_field_attribute.append(str(one_field_attribute_dict["attr_record_counter"]))
                    one_field_attribute.append(str(one_field_attribute_dict["AttributeSize"]))
                    one_field_attribute.append(str(one_field_attribute_dict["Attribute_field_NameSize"]))
                    one_field_attribute.append(str(one_field_attribute_dict["Attribute_field_ValueSize"]))
                    one_field_attribute.append(str(modelElement["attribute_counter"]))
                    one_field_attribute.append(str(one_field_attribute_dict["AttributeUUID"]))
                    one_field_attribute.append(str(one_field_attribute_dict["ElementUUID"]))

                    #sq_field_stat.extend(one_field_attribute)
                    csv_line_attribute_stat=spr_field_stat.join(one_field_attribute)
                    csv_attribute_stat+=csv_line_attribute_stat+"\n"
                #*******************************
            #print(csv_line)
                if count % 1000  == 0 or count==1:
                    model_stat_field_stat.write(csv_field_stat)
                    csv_field_stat=""
                    model_stat_attribute_stat.write(csv_attribute_stat)
                    csv_attribute_stat=""
                    print(count)
                    t.stop()
                    print(t.elapsed)
                    t.reset()
                    t.start()
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                # if count>=2000:
                #     break
        model_stat_field_stat.write(csv_field_stat)
    del context
    with open("model_stat.csv","w",encoding="utf-8")as model_stat:
        model_stat.write(csv)
    #with zipfile.ZipFile("model_stat_field_stat.zip", mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
    #        zf.writestr("model_stat_field_stat.csv", csv_field_stat.encode("utf-8"))
    with zipfile.ZipFile("model_stat_field_stat.zip", mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write("model_stat_field_stat.csv")
    os.remove("model_stat_field_stat.csv")
    with zipfile.ZipFile("model_stat_attribute_stat.zip", mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write("model_stat_attribute_stat.csv")
    os.remove("model_stat_attribute_stat.csv")
     #with open("model_stat_field_stat.csv","w",encoding="utf-8")as model_stat_field_stat:
     #    model_stat_field_stat.write(csv_field_stat)

    print(count)
    t.stop()
    print(t.elapsed)
    time_total.stop()
    print("Total import time:{}".format(time_total.elapsed))
    print("Total elements:{}".format(count))
    #print("Elements with dot in field name:{}".format(elements_with_dot_count))
    #print("Inserted documents:{}".format(model.count()))
    #db.orders.count()
    err_msg_uniq=uniq(err_msg.values())
    print("Elements with dot or $  in field name:{}".format(len(err_msg)))
    print("Unique errors :{}".format(len( err_msg_uniq)))
    for msg in err_msg_uniq:
        print(msg)
    exit(0)


