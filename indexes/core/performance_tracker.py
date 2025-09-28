import time

class OperationResult:
    def __init__(self, data, execution_time_ms, disk_reads, disk_writes):
        self.data = data
        self.execution_time_ms = execution_time_ms
        self.disk_reads = disk_reads
        self.disk_writes = disk_writes
        self.total_disk_accesses = disk_reads + disk_writes

    def __repr__(self):
        return f"OperationResult(data={self.data}, time={self.execution_time_ms:.2f}ms, accesses={self.total_disk_accesses})"

class PerformanceTracker:
    def __init__(self):
        self.reset()

    def reset(self):
        self.reads = 0
        self.writes = 0
        self.start_time = 0

    def start_operation(self):
        self.reads = 0
        self.writes = 0
        self.start_time = time.time()

    def track_read(self):
        self.reads += 1

    def track_write(self):
        self.writes += 1

    def end_operation(self, result_data):
        execution_time = (time.time() - self.start_time) * 1000
        return OperationResult(result_data, execution_time, self.reads, self.writes)