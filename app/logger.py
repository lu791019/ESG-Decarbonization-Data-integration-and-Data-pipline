import logging

FORMAT = '%(asctime)s %(filename)s %(name)s %(levelname)s:%(message)s'

# logging.basicConfig(level=logging.INFO, format=FORMAT, filename='log.log')
logging.basicConfig(level=logging.INFO, format=FORMAT)
# logger = logging.getLogger('sqlalchemy.engine')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('start')
