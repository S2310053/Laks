# This module constructs the final dataset to be used in modelling

##
# The function of this code is to create a csv file with all the features (Factors.csv) that will be used by the model scripts
# Since we have FishPool data from Jan-06 to Apr-26, the file contains data only from this period
##

import os
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from data_loader import DataLoader
from feature_engineer import FeatureEngineer
from eda import EDA

# Dataset starting on 01-01-2006
DATA_START   = "2004-01-01"
DATA_END     = "2026-04-01"
EXPORT_START = "2006-01-01"

# Load raw data
loader  = DataLoader()
feature = FeatureEngineer()

# Prints validation results
data = loader.Data()
loader.ValidateData(data)

# Build feature matrix
Factors, freq_map = feature.buildFeatureMatrix(data)
feature.validateFeatureMatrix(Factors)

Factors = Factors[
    (Factors["Date"] >= DATA_START) &
    (Factors["Date"] <= DATA_END)
].reset_index(drop=True)

# Export Factor.csv (Used by all model scripts)
out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "Factors.csv")
os.makedirs(os.path.dirname(out_path), exist_ok=True)
Factors[Factors["Date"] >= EXPORT_START].to_csv(out_path, index=False)
print(f"Factors.csv saved → {out_path}")
