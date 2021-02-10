import configparser as cfp
from importlib import resources

import pandas as pd

pd.options.display.max_columns = None
pd.options.display.width = None

cfg = cfp.ConfigParser(interpolation=cfp.ExtendedInterpolation())

with resources.path('config', 'config.ini') as path:
    cfg.read(path)