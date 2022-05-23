class GefranPidError(Exception):
    """
    General Gefran PID error
    """

    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self):
        return self.msg
