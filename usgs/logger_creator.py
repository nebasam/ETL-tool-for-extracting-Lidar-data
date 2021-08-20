import logging


class CreateLogger:
    def __init__(self, name: str, handlers: int = 1, file_name: str = 'log.log') -> None:
        # Create a custom logger
        self.logger = logging.getLogger(name)
        self.num_handlers = handlers
        self.file_name = file_name

    def create_handlers(self) -> None:
        if(self.num_handlers == 1):
            # Create Only StreamHandler for console logging
            self.create_console_handler()
            self.cons_handler.setFormatter(self.create_handle_format(0))
            self.add_handlers([self.cons_handler])

        elif(self.num_handlers == 2):
            # Create Only FileHandler for file logging
            self.create_file_handler(self.file_name)
            self.file_handler.setFormatter(self.create_handle_format(1))
            self.add_handlers([self.file_handler])
        else:
            # Create Both Handlers
            self.create_console_handler()
            self.create_file_handler(self.file_name)
            self.cons_handler.setFormatter(self.create_handle_format(0))
            self.file_handler.setFormatter(self.create_handle_format(1))
            self.add_handlers([self.cons_handler, self.file_handler])

    def create_console_handler(self) -> None:
        self.cons_handler = logging.StreamHandler()
        self.cons_handler.setLevel(logging.DEBUG)

    def create_file_handler(self, file) -> None:
        self.file_handler = logging.FileHandler(filename=file)
        self.file_handler.setLevel(logging.ERROR)

    def create_handle_format(self, type: int = 0) -> logging.Formatter:
        if(type == 0):
            return logging.Formatter('%(name)s:%(levelname)s->%(message)s')
        elif(type == 1):
            return logging.Formatter('%(asctime)s:%(name)s:%(levelname)s->%(message)s')
        else:
            return logging.Formatter('%(levelname)s->%(message)s')

    def add_handlers(self, handle_list: list) -> None:
        for handle in handle_list:
            self.logger.addHandler(handle)

    def get_default_logger(self) -> logging.Logger:
        # Run create_handlers
        self.create_handlers()
        self.logger.setLevel(logging.INFO)

        return self.logger
