from logging.handlers import TimedRotatingFileHandler
import logging


def getLogger(name: str):

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        filename="omnivore.log",
        filemode="a",
    )

    rotate = TimedRotatingFileHandler("omnivore.log", "D", backupCount=30)
    logger = logging.getLogger(name)
    logger.addHandler(rotate)
    return logger
