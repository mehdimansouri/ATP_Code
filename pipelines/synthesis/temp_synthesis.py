import datetime as dt
import logging
import numpy as np
import os
import pandas as pd
import time
import rpy2

from rpy2 import robjects
from rpy2.robjects import packages
from typing import Any, Dict, List

robjects.r['options'](warn=-1)
utils = robjects.packages.importr('utils')
utils.chooseCRANmirror(ind=1)

# utils.install_packages('changepoint')
utils.install_packages('changepoint.np')

# robjects.r("library(changepoint)")
robjects.r("library(changepoint.np)")
