## 
#  This program implements the stage of modelling
#

##
# Imports libraries needed
#
from data_loader import DataLoader

import matplotlib
#matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import numpy as np

import pandas as pd
import seaborn as sns

##
# Loads the data
#
loadData              = DataLoader()

data = loadData.Data()

#print(data)
#print(data.columns)
#print(data.info())

#loadData.ValidateData(data)
