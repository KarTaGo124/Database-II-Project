# Extendible Hashing Index
import struct
from types import SimpleNamespace

class Serializer:
    def __init__(self, record_format):
        self.record_format = record_format
        #example  [('id', 'i'), ('name', '20s'), ('grades', '*f')]
        # use * for variable length type

    def pack(self, record):
        pack = []
        for (name, format) in self.record_format:
            val = getattr(record, name) #we get from the SimpleNameSpace object the value of the attribute specified
                #in the record-format given by the parser
            if format[0] == '*':
                n = len(val)
                 #doesnt support array of strings
                pack.append(struct.pack('i', n)) #we pack the length of the variable and pack it to know the end of it
                pack.append(struct.pack(f'{n}' + format[1:], *val)) #pack all values at once
            else:
                if 's' in format: #20s eg.
                    val = val.encode()
                pack.append(struct.pack(format, val))

        return b''.join(pack)

    @staticmethod
    def unpack(self, data):
        record = SimpleNamespace()
        for (name, format) in self.record_format: #according to the format
            if format[0] == '*': #if its varialbe length
                n = struct.unpack('i', data[:4])[0] #we get the amount, [0] because it returns a tuple
                val = struct.unpack(f'{n}' + format[1:], data[4:4+n*struct.calcsize(format[1:])]) #multiple values, dont use [0]
                #then we unpack the quantity after the int and until the total elements using calcsize
                val = list(val)
                data = data[4+n*struct.calcsize(format[1:]):] #slice each time we do this so we dont use an offset
            else:
                val = struct.unpack(format, data[:struct.calcsize(format)])[0]
                if 's' in format:
                    val = val.decode().strip('\x00') #decode if string
                #we unpack according to format
                # till the size of the format
                data = data[struct.calcsize(format):] #slice data
            setattr(record, name, val) #we set the attributes
        return record


