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

DATA = loadData.Data()
print(DATA)
print("RUNNING SCRIPT:", __name__)


#data = loadData.loadExportData()

#print(data)
#print(data.columns)