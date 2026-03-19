## 
#  This program implements the stage of modelling
#

##
# Imports libraries needed
#
from data_loader import DataLoader
from feature_engineer import featureEngineer

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import numpy as np

import pandas as pd
import seaborn as sns

##
# Loads the data
#
loadData              = DataLoader()
feature               = featureEngineer()

data = loadData.Data()
loadData.ValidateData(data)
data = feature._lagByPublication(data)


#print(data)
#print(data.columns)
#print(data.info())



data.to_csv(r"C:\Users\arzol\OneDrive\Escritorio\Work\Master Thesis\Laks\Data\data.csv", index = False)


