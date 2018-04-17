class ModelError(Exception):
    """Base error class.

    :param message: Error message.
    :param args: optional Message formatting arguments.

    """

    def __init__(self, message, *args):
        super(ModelError, self).__init__(message % args if args else message)
