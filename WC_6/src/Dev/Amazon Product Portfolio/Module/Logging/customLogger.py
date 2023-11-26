import logging
import os
import sys

class CustomLogger(logging.Logger):
    def __init__(self, name, log_file):
        super().__init__(name)
        self.log_file = log_file

        # Create a log formatter with the desired format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Create a console handler to display log messages to the console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.addHandler(console_handler)

        # Create a file handler to write log messages to the log file
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        self.addHandler(file_handler)