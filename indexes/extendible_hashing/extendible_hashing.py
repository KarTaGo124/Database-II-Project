# Extendible Hashing Index
import struct
BLOCK_FACTOR = 4
RECORD_SIZE = 100
#max overflow 1 bucket
class Bucket:
    HEADER_FORMAT = "iii"  # local_depth, size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def _init_(self, local_depth=1, size=0, next_bucket=None):
        self.local_depth = local_depth
        self.size = size
        self.next_bucket = next_bucket

    def insert(self, data: bytes, file, bucket_pos: int):
        # 1) check if size < block factor
        if self.size >= BLOCK_FACTOR:
            return False #split or overflow
        # 2) write record
        file.seek(bucket_pos + self.HEADER_SIZE + self.size*RECORD_SIZE)
        file.write(data)

        self.size += 1

        file.seek(bucket_pos)

        file.write(struct.pack(
            self.HEADER_FORMAT,
            self.local_depth,
            self.size,
            self.next_bucket
        ))

        return True


