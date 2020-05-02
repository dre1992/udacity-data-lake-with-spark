

class JobContext(object):
    """
    The JobContext object holds shared data across multiple jobs and can be used for creating accumulators and
    broadcast variables

    :param sc: The spark session
    """


    def __init__(self, sc):
        self._init_shared_data(sc)

    def _init_shared_data(self, sc):
        pass


