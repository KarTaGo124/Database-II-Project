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
            val = getattr(record, name) #get val of attr
            if format[0] == '*':
                n = len(val)
                 #doesnt support array of strings
                pack.append(struct.pack('i', n)) #we pack the length of the variable and pack it to know the end of it
                pack.append(struct.pack(f'{n}' + format[1:], *val)) #pack all values at once
            else:
                if 's' in format: #20s eg.
                    val = val.encode()
                #we get from the SimpleNameSpace object the value of the attribute specified
                #in the record-format given by the parser
                pack.append(struct.pack(format, val))

        return b''.join(pack)