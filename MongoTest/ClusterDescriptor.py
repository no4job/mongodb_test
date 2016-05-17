__author__ = 'mdu'
class ClusterDescriptor():
    ABC="abcdefghijklmnopqrstuvwxyz"
    def __init__(self,**kwargs):
        self.cluster=int(kwargs.get('cluster',''))
        self.document_number=int(kwargs.get('document_number',''))
        self.s1_name_size=int(kwargs.get('s1_name_size',''))
        self.s1_description_size=int(kwargs.get('s1_description_size',''))
        self.s1_type_size=int(kwargs.get('s1_type_size',''))
        self.field_x_name_size=int(kwargs.get('field_x_name_size',''))
        self.field_x_number=int(kwargs.get('field_x_number',''))
        self.field_x_x_name_size=int(kwargs.get('field_x_x_name_size',''))
        self.field_x_x_value_size=int(kwargs.get('field_x_x_value_size',''))
        self.field_x_x_number=int(kwargs.get('field_x_x_number',''))
        self.empty_field_x_ratio=float(kwargs.get('empty_field_x_ratio','').replace(",","."))
        self.types_number=int(kwargs.get('types_number',''))

    def convert_to_ABC(self,number):
        result =ClusterDescriptor.ABC[number % len(ClusterDescriptor.ABC)]
        while True:
            reminder=number // len(ClusterDescriptor.ABC)
            if reminder > 0:
                result+=ClusterDescriptor.ABC[number % len(ClusterDescriptor.ABC)]
                number = reminder
            else:
                return result

    def get_s1_name(self,elementNumber,maxValue=None):
       if maxValue==None:
           maxValue=self.s1_name_size
       clusterABC=self.convert_to_ABC(self.cluster)
       suffixLength=maxValue-(len(clusterABC)+len(str(elementNumber)))
       if suffixLength<0:
        raise ValueError('Cluster name + element number string size = {} is more than max value ={}',
                         format(len(clusterABC)+len(str(elementNumber)), maxValue))
       suffix="x"*suffixLength
       return clusterABC+str(elementNumber)+suffix

    def get_s1_description(self,elementNumber):
        try:
            return self.get_s1_name(elementNumber,self.s1_description_size)
        except:
            raise

    def get_s1_type(self,typeNumber):
        suffixLength=self.s1_type_size-len(str(typeNumber))
        if suffixLength<0:
            raise ValueError('Type number string size = {} is more than max value ={}',
                             format(len(str(typeNumber)), self.s1_type_size))
        suffix="x"*suffixLength
        return str(typeNumber)+suffix


    def get_field_x_name(self,fieldNumber,maxValue=None):
        if maxValue==None:
            maxValue=self.field_x_name_size
        suffixLength=maxValue-len(str(fieldNumber))-1
        if suffixLength<0:
            raise ValueError('Prefix + field number string size = {} is more than max value ={}',
                         format(len(str(fieldNumber))+1, maxValue))
        suffix="x"*suffixLength
        return "f"+str(fieldNumber)+suffix

    def get_field_x_x_name(self,fieldNumber):
        try:
            return self.get_field_x_name(fieldNumber,self.field_x_x_name_size)
        except:
            raise
    def get_field_x_x_value(self,fieldNumber):
        suffixLength=self.field_x_x_value_size-len(str(fieldNumber))
        if suffixLength<0:
            raise ValueError('Field number string size = {} is more than max value ={}',
                             format(len(str(fieldNumber)), self.field_x_x_value_size))
        suffix="x"*suffixLength
        return str(fieldNumber)+suffix