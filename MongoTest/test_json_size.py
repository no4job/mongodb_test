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

def get_model_element(elem):
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

    modelElement["name"]=next(iter(elem.xpath("./@ElementName")),"")
    modelElement["id"]=next(iter(elem.xpath("./@ElementId")),"")
    modelElement["type"]=next(iter(elem.xpath("./@TypeID")),"")
    data_section_2=[]
    record_counter=0
    value_counter=0
    field_stat=[]
    for attributeElement in elem.xpath("./www.qpr.com:Attribute",namespaces={'www.qpr.com': 'www.qpr.com'}):
        fieldName=next(iter(attributeElement.xpath("./@AttributeName")),"")
        fieldName=fieldName.replace('.', '\uff0E')
        field={}
        field_values_array=[]
        valueArray=[]
        valueArray.extend(attributeElement.xpath("./www.qpr.com:Value/text()",namespaces={'www.qpr.com': 'www.qpr.com'}))

        if len(valueArray):
            field_values_array.append(dict(Value=valueArray))
            value_counter+=1
        records=[]
        for recordElement in attributeElement.xpath("./www.qpr.com:Record",namespaces={'www.qpr.com': 'www.qpr.com'}):
            #record=[]
            record={}
            for recordFieldElement in recordElement.xpath("./www.qpr.com:Field",namespaces={'www.qpr.com': 'www.qpr.com'}):
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
                field_stat.append(one_field_stat)
            records.append(record)
        if len(records):
            field_values_array.append(dict(Record=records))
        field[fieldName]=field_values_array
        data_section_2.append(field)
    modelElement["data_section_2"]=data_section_2
    result={}
    result["modelElement"]= modelElement
    result["record_counter"]= record_counter
    result["value_counter"]= value_counter
    result["element_size"]=len(str(json.dumps(modelElement,ensure_ascii=False)))
    result["field_stat"]=field_stat
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
    spr_field_stat=","
    head="name"+spr+"id"+spr+"type"+spr+"element_size"+spr+"record_counter"+spr+"value_counter"+"\n"
    csv=head
    head="AttributeName_size"+spr_field_stat+"field_name_size"+spr_field_stat+"field_value_size"+spr_field_stat+"element_size"+spr_field_stat+"type"+"\n"
    csv_field_stat=head
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
            sq.append(modelElement["modelElement"]["name"])
            sq.append(modelElement["modelElement"]["id"])
            sq.append(modelElement["modelElement"]["type"])
            sq.append(str(modelElement["element_size"]))
            sq.append(str(modelElement["record_counter"]))
            sq.append(str(modelElement["value_counter"]))
            #sq_field_stat=[]
            for one_field_stat_dict in modelElement["field_stat"]:
                one_field_stat=[]
                one_field_stat.append(str(one_field_stat_dict["AttributeName_size"]))
                one_field_stat.append(str(one_field_stat_dict["field_name_size"]))
                one_field_stat.append(str(one_field_stat_dict["field_value_size"]))
                one_field_stat.append(str(modelElement["element_size"]))
                one_field_stat.append(modelElement["modelElement"]["type"])
                #sq_field_stat.extend(one_field_stat)
                csv_line_field_stat=spr_field_stat.join(one_field_stat)
                csv_field_stat+=csv_line_field_stat+"\n"
            csv_line=spr.join(sq)
            csv+=csv_line+"\n"
        #print(csv_line)
            if count % 1000  == 0 or count==1:
                print(count)
                t.stop()
                print(t.elapsed)
                t.reset()
                t.start()
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
            # if count>=1000:
            #     break
    del context
    with open("model_stat.csv","w",encoding="utf-8")as model_stat:
        model_stat.write(csv)
    with zipfile.ZipFile("model_stat_field_stat.zip", mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("model_stat_field_stat.csv", csv_field_stat.encode("utf-8"))
    # with open("model_stat_field_stat.csv","w",encoding="utf-8")as model_stat_field_stat:
    #     model_stat_field_stat.write(csv_field_stat)

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


