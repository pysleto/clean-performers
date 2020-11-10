import configparser as cfp
from importlib import resources

cfg = cfp.ConfigParser(interpolation=cfp.ExtendedInterpolation())

with resources.path('config', 'config.ini') as path:
    cfg.read(path)