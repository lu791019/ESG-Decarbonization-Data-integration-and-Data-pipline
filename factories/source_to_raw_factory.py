from app.logger import logger
from factories.SourceToRawFactoryException import SourceToRawFactoryException
from jobs.source_to_raw.fem_ratio import main as fem_ratio
from jobs.source_to_raw.fem_ratio_solar import main as fem_ratio_solar
from jobs.source_to_raw.solar import main as solar
from jobs.source_to_raw.solar_ratio import main as solar_ratio


def main(name):
    try:
        if name == 'fem_ratio':
            logger.info('fem_ratio start.')
            return fem_ratio()
        elif name == 'fem_ratio_solar':
            logger.info('fem_ratio_solar start.')
            return fem_ratio_solar()
        elif name == 'solar_ratio':
            logger.info('solar_ratio start.')
            return solar_ratio()
        elif name == 'solar':
            logger.info('solar start.')
            return solar()
        else:
            raise ValueError('source to raw factory: Invalid name')
    except Exception as error:
        raise SourceToRawFactoryException(name, error) from error
