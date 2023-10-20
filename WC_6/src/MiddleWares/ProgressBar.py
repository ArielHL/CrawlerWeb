import threading
from tqdm import tqdm

class CustomProgressBar:
    def __init__(self, total, desc, leave=True, position=None):
        self.total = total
        self.desc = desc
        self.leave = leave
        self.current = 0
        self.position = position  # Assign a fixed position based on thread ID
        self.lock = threading.Lock()

        # Initialize the tqdm instance at 0% with a fixed position
        self.progress_bar = tqdm(total=total, desc=desc, leave=leave, initial=0, position=self.position)

    def update(self, value):
        with self.lock:
            self.current += value
            self.progress_bar.n = self.current
            self.progress_bar.last_print_n = self.current
            self.progress_bar.update(0)  # Update the progress bar without incrementing the total

    def close(self):
        self.progress_bar.close()
