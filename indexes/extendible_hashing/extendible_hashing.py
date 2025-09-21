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
            val = getattr(record, name)  #we get from the SimpleNameSpace object the value of the attribute specified
            #in the record-format given by the parser
            if format[0] == '*':
                n = len(val)
                #doesnt support array of strings
                pack.append(struct.pack('i', n))  #we pack the length of the variable and pack it to know the end of it
                pack.append(struct.pack(f'{n}' + format[1:], *val))  #pack all values at once
            else:
                if 's' in format:  #20s eg.
                    val = val.encode()
                pack.append(struct.pack(format, val))

        return b''.join(pack)

    @staticmethod
    def unpack(data,record_format):
        record = SimpleNamespace()
        for (name, format) in record_format:  #according to the format
            if format[0] == '*':  #if its varialbe length
                n = struct.unpack('i', data[:4])[0]  #we get the amount, [0] because it returns a tuple
                val = struct.unpack(f'{n}' + format[1:],
                                    data[4:4 + n * struct.calcsize(format[1:])])  #multiple values, dont use [0]
                #then we unpack the quantity after the int and until the total elements using calcsize
                val = list(val)
                data = data[4 + n * struct.calcsize(format[1:]):]  #slice each time we do this so we dont use an offset
            else:
                val = struct.unpack(format, data[:struct.calcsize(format)])[0]
                if 's' in format:
                    val = val.decode().strip('\x00')  #decode if string
                #we unpack according to format
                # till the size of the format
                data = data[struct.calcsize(format):]  #slice data
            setattr(record, name, val)  #we set the attributes
        return record


#max overflow 1 bucket
class Bucket:
    BLOCK_SIZE = 4096  # 4KB block size
    HEADER_FORMAT = "iiii"  # local_depth, record pointer, slot size, next
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SLOT_FORMAT = "ii"  # offset, length of each record
    SLOT_SIZE = struct.calcsize(SLOT_FORMAT)
    # we will use every space available to save records until it doesnt fit

    def __init__(self, local_depth=1,slot_size=0, record_pointer=BLOCK_SIZE,  next_bucket=None):
        self.local_depth = local_depth
        self.slot_size = slot_size
        self.record_pointer =  record_pointer
        self.next_bucket = next_bucket

    def get_free_space(self):
        return self.record_pointer - self.HEADER_SIZE - self.SLOT_SIZE * self.slot_size

    def insert(self, data: bytes, file, bucket_pos: int):
        length = len(data)
        if length > self.get_free_space():
            return False #check if space for record
        new_record_pointer = self.record_pointer - length
        slot_pos = self.HEADER_SIZE + self.slot_size * self.SLOT_SIZE
        # check if slot and record will cross each other
        if new_record_pointer < slot_pos:
            return False # no room for both so split or overflow
        # 1) reserve space
        self.record_pointer = new_record_pointer

        # 2) write record payload
        file.seek(bucket_pos + self.record_pointer)
        file.write(data)

        # 3) write new slot entry
        file.seek(bucket_pos + slot_pos)
        file.write(struct.pack(self.SLOT_FORMAT, self.record_pointer, length))

        # 4) update slot count
        self.slot_size += 1

        # 5) update header (with new record_pointer!)
        file.seek(bucket_pos)
        file.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.slot_size,
            self.record_pointer,
            self.next_bucket
        ))

        return True
