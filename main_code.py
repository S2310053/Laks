## 
#  This program implements the stage of modelling
#

##
# Imports libraries needed
#
from data_loader import DataLoader

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

data = loadData.Data()

#print(data)
#print(data.columns)
#print(data.info())


loadData.ValidateData(data)
data.to_excel(r"C:\Users\arzol\OneDrive\Escritorio\data.xlsx", index = False)

#plt.plot(data["Date"], data["Salmon_Exported_Tons_SSB_Weekly"])

correl = data.corr()

correl.to_excel(r"C:\Users\arzol\OneDrive\Escritorio\correlation.xlsx", index = True)

