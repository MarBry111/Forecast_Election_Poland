import numpy as np
import pandas as pd

import os
from poll_data import party_in_region, region_in_party

import pickle

from scipy.optimize import curve_fit

def model_percent(a,x):
    '''
    Function runs for each year seperatly (or for all at once)
    INPUT:
    a - vector of weights n_districts x n_parameters
    x - vector of input data years (or 1) x n_districts x n_parameters
    OUTPUT:
    y - predicted values in (0,1)
    '''    
    a = np.repeat([a], x.shape[0], 0)
    y = 1 / (1+np.exp(-np.sum(x*a, 1, keepdims=True) ))
    return y

def grad_percent(a,x,y):
    '''
    INPUT:
    a - vector of weights n_districts x n_parameters
    x - vector of input data years (or 1) x n_districts x n_parameters
    '''
    #return a * np.exp(-x.T.dot(a)) / (1+np.exp(-x.T.dot(a)))**2
    #return a*np.exp(-np.sum(x*a,1,keepdims=True)) / (1+np.exp(-np.sum(x*a,1,keepdims=True)))**2
    d0 = x.shape[0] if (len(x.shape) == 3) else 1
    
    a = np.repeat(a, d0, 0)
    x = x.reshape(-1, 3)
    y = y.reshape(-1, 1)
    y1 = -(2 * 
          ( y - 1/(1+np.exp(-np.sum(x*a,1,keepdims=True))) ) * 
          1/(1+np.exp(-np.sum(x*a,1,keepdims=True)))**2 *
          np.exp(-np.sum(x*a,1,keepdims=True)) *
          x)
    
    return y1