##
#  Entry point — orchestrates the full data pipeline
#
#  Stage 0: Load raw data        (DataLoader)
#  Stage 1: Build feature matrix (FeatureEngineer — lags + transforms internally)
#  Stage 2: EDA                  (EDA — commented out, run separately)
#  Stage 3: Export Factors.csv   (consumed by model scripts)
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

## ── Pipeline configuration ───────────────────────────────────────────────────
#  DATA_START    : earliest row kept after building the feature matrix.
#                  Set 2 years before EXPORT_START to warm up rolling features
#                  (52-week rolling windows need ~1 year; 2 years gives margin).
#  EXPORT_START  : first row written to Factors.csv and used by model scripts.
#                  2006 ensures all rolling features are fully populated.
DATA_START   = "2004-01-01"
DATA_END     = "2026-04-01"
EXPORT_START = "2006-01-01"

## ── Stage 0: Load raw data ───────────────────────────────────────────────────
loader  = DataLoader()
feature = FeatureEngineer()

data = loader.Data()
loader.ValidateData(data)

## ── Stage 1: Build feature matrix ────────────────────────────────────────────
#  buildFeatureMatrix() applies publication lags and validates them internally.
Factors, freq_map = feature.buildFeatureMatrix(data)
feature.validateFeatureMatrix(Factors)

Factors = Factors[
    (Factors["Date"] >= DATA_START) &
    (Factors["Date"] <= DATA_END)
].reset_index(drop=True)

## ── Stage 2: EDA (run separately when needed) ────────────────────────────────
# eda = EDA(Factors, freq_map=freq_map)
# eda.report()

## ── Stage 3: Export ──────────────────────────────────────────────────────────
out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data", "Factors.csv")
os.makedirs(os.path.dirname(out_path), exist_ok=True)
Factors[Factors["Date"] >= EXPORT_START].to_csv(out_path, index=False)
print(f"Factors.csv saved → {out_path}")
