
import logging as lgg
import datetime as dt
import shared.directory_names as dn

def get_logger(**kwargs):

    now_datetime = dt.datetime.now()

    folder_string = dn.get_dated_directory_extension(ext='log',folder_date = int(now_datetime.strftime('%Y%m%d')))


    file_identifier = kwargs['file_identifier']

    if 'log_level' in kwargs.keys():
        log_level = kwargs['log_level']
    else:
        log_level = 'WARNING'

    if log_level.upper()=='CRITICAL':
        log_level = lgg.CRITICAL
    elif log_level.upper()=='ERROR':
        log_level = lgg.ERROR
    elif log_level.upper()=='WARNING':
        log_level = lgg.WARNING
    elif log_level.upper()=='INFO':
        log_level = lgg.INFO
    elif log_level.upper() == 'DEBUG':
        log_level = lgg.DEBUG


    logger = lgg.getLogger(__name__)
    logger.setLevel(log_level)

    handler = lgg.FileHandler(folder_string + '/' + now_datetime.strftime('%Y%m%d_%H%M%S') + '_' + file_identifier + '.log')
    handler.setLevel(log_level)

    logger.addHandler(handler)
    return logger