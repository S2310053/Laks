##
#  This module engineers the features constructed on selected variables
#

##
#  Import necessary libraries
#
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats

##
#  This class composes the feature engineering, 
#  constructs variables used by the models
#
_fileName = r"Data/data.xlsx"
data = pd.read_excel(_fileName, header = 0)


## Build salmon price feature

salmonCarbon = data[["Date", "Salmon_NOK_kg_FP_Weekly"  ,"Salmon_Exported_Tons_SSB_Weekly"]].dropna()

salmon = np.log(salmonCarbon["Salmon_NOK_kg_FP_Weekly"]).diff().dropna()
carbon = np.log(salmonCarbon["Salmon_Exported_Tons_SSB_Weekly"]).diff().dropna()

corr = salmon.corr(carbon)

print(corr)
