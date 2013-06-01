class ServiceException(Exception):
    def __init__(self, message, code=None):
        Exception.__init__(self, message)
        self.Message = message
        self.Code = code

    def __str__(self):
        return self.Message + " (code " + str(self.Code) + " )"

class ServiceWarning(ServiceException):
    pass

class APIException(ServiceException):
    def __init__(self, message, code=None):
        ServiceException.__init__(self, message, code=code)

class APIWarning(ServiceWarning):
    pass

class APIAuthorizationException(APIException):
    pass
