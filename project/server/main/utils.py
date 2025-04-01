from project.server.main.logger import get_logger
from collections import Counter
logger = get_logger(__name__)

def get_mbytes(z):
    if not isinstance(z, str):
        return None
    x = z.lower().replace(',', '').replace(' ', '')
    for k in ['bytes', 'byte']:
        if k in x:
            y = x.replace(k, '').strip()
            try:
                nb_bytes = float(y)
                nb_mo = (nb_bytes/1000000)
                return {'mo': nb_mo, 'unit_detected': k}
            except:
                logger.debug(f"unable to get size from {x}")
                return None
    for k in ['ko', 'kb', 'kbyte', 'kbytes', 'kio', 'kib', 'kilobytes']:
        if k in x:
            y = x.replace(k, '').strip()
            try:
                nb_kbytes = float(y)
                nb_mo = (nb_kbytes/1000)
                return {'mo': nb_mo, 'unit_detected': k}
            except:
                logger.debug(f"unable to get size from {x}")
                return None
    for k in ['mo', 'mb', 'mib', 'mio', 'mbyte', 'mbytes', 'megabytes']:
        if k in x:
            y = x.replace(k, '').strip()
            try:
                nb_mbytes = float(y)
                nb_mo = (nb_mbytes)
                return {'mo': nb_mo, 'unit_detected': k}
            except:
                logger.debug(f"unable to get size from {x}")
                return None
    for k in ['go', 'gb', 'gib', 'gbyte', 'gbytes', 'gigabytes']:
        if k in x:
            y = x.replace(k, '').strip()
            try:
                nb_gbytes = float(y)
                nb_mo = (nb_gbytes*1000)
                return {'mo': nb_mo, 'unit_detected': k}
            except:
                logger.debug(f"unable to get size from {x}")
                return None
    for k in ['to', 'tb', 'tib', 'tbyte', 'tbytes', 'terabytes']:
        if k in x:
            y = x.replace(k, '').strip()
            try:
                nb_bytes = float(y)
                nb_mo = (nb_gbytes*1000000)
                return {'mo': nb_mo, 'unit_detected': k}
            except:
                logger.debug(f"unable to get size from {x}")
                return None
    for k in ['page', 'datapoint', 'station', 'word', 'interaction', 'file', 'earthquake', 'dataset', 'spectrum', 'spectra']:
        if k in x:
            return {'unit_detected': k}
    logger.debug(f"unable to get size from {x}")
    return None

def get_most_frequent(a):
    b = Counter(a)
    try:
        return b.most_common(1)[0][0]
    except:
        return None
