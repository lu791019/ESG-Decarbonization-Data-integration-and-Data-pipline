
class ShipmentException(Exception):
    def __init__(self, code):
        self.message = 'shipment exception'
        self.code = code
        
        if (code == '3001'):
            self.message = 'file type error'

        super().__init__(self.message)
