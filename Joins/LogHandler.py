import logging

logger = logging
   
# logging basic config method and saving log files
logger.basicConfig(filename='joins.log', level=logging.ERROR,
format='%(asctime)s:%(funcName)s:%(levelname)s:%(lineno)d')