import time

class OperationResult:
    def __init__(self, data, execution_time_ms, disk_reads, disk_writes, rebuild_triggered=False):
        self.data = data
        self.execution_time_ms = execution_time_ms
        self.disk_reads = disk_reads
        self.disk_writes = disk_writes
        self.total_disk_accesses = disk_reads + disk_writes
        self.rebuild_triggered = rebuild_triggered

    def __repr__(self):
        rebuild_info = " [REBUILD]" if self.rebuild_triggered else ""
        return f"OperationResult(data={self.data}, time={self.execution_time_ms:.2f}ms, accesses={self.total_disk_accesses}{rebuild_info})"

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

    def end_operation(self, result_data, rebuild_triggered=False):
        execution_time = (time.time() - self.start_time) * 1000
        return OperationResult(result_data, execution_time, self.reads, self.writes, rebuild_triggered)