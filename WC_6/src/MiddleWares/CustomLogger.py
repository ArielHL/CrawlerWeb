import logging

class CustomLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET,logger_file=None):
        super().__init__(name, level)
        self.file_handler = logging.FileHandler(logger_file)
        self.file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.addHandler(self.file_handler)

        self.terminal_handler = logging.StreamHandler()
        self.terminal_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    def enable_terminal_logging(self):
        self.addHandler(self.terminal_handler)

    def disable_terminal_logging(self):
        self.removeHandler(self.terminal_handler)


