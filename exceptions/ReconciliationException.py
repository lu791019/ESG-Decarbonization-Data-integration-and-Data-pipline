class ReconciliationException(Exception):
    def __init__(self, code):
        self.message = 'reconciliation exception'
        self.code = code

        if (code == '4001'):
            self.message = 'file type error'

        super().__init__(self.message)
