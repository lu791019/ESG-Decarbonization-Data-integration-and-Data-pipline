
class SourceToRawFactoryException(Exception):
    def __init__(self, name, message):
        super().__init__('[source_to_raw]{} => {}'.format(name, message))
