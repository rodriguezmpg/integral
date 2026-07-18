import time as _time
import logging
from collections import deque


class DequeHandler(logging.Handler):
    def __init__(self, maxlines=1000):
        super().__init__()
        self.buffer = deque(maxlen=maxlines)

    def emit(self, record):
        self.buffer.append(self.format(record))


class NoisyRequestFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        if "GET" in msg or "POST" in msg or "DELETE" in msg:
            return False
        if "/datos" in msg or "/precio" in msg:
            return False
        return True


def setup_logging():
    handler = DequeHandler(maxlines=1000)
    handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    formatter.converter = _time.gmtime  # UTC

    handler.setFormatter(formatter)

    logger = logging.getLogger('reg')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    werkzeug_logger = logging.getLogger('werkzeug')
    if not any(isinstance(f, NoisyRequestFilter) for f in werkzeug_logger.filters):
        werkzeug_logger.addFilter(NoisyRequestFilter())

    return handler


log_handler = setup_logging()