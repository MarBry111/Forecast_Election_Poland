import numpy as np
import pandas as pd

import os
from poll_data import party_in_region, region_in_party

import pickle

from scipy.optimize import curve_fit

'''
 # https://stackoverflow.com/questions/59845950/how-to-use-cross-validation-with-curve-fit
 Cross validation for data all vs 1 -> then use parameters to get vlaue for first year

 # https://towardsdatascience.com/time-series-nested-cross-validation-76adba623eb9
 nested cross validation for time series datas
'''