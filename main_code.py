## 
#  This program implements the stage of modelling
#

##
# Imports libraries needed
#
from data_loader import DataLoader
from feature_engineer import featureEngineer
from eda import EDA

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
feature.validatePublicationLags(data)
Factors, freq_map = feature.buildFeatureMatrix(data)
feature.validateFeatureMatrix(Factors)

Factors = Factors[
    (Factors["Date"] >= "2004-01-01") &
    (Factors["Date"] <= "2025-12-31")
].reset_index(drop=True)

eda = EDA(Factors, freq_map=freq_map)
eda.report()

#print(data)
#print(data.columns)
#print(data.info())

import os as _os
_out = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "Data", "Factors.csv")
_os.makedirs(_os.path.dirname(_out), exist_ok=True)
Factors[Factors["Date"] >= "2006-01-01"].to_csv(_out, index=False)
print(f"Factors.csv saved → {_out}")