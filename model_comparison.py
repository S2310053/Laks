##
#  Model comparison on Factors.csv
#  Walk-forward cross-validation across multiple models
##

import pandas as pd
import numpy as np
from catboost import CatBoostRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from sklearn.linear_model import Lasso, Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

## Load data
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
df = df.dropna(subset=["Salmon (NOK/KG)"]).reset_index(drop=True)

TARGET   = "Salmon (NOK/KG)"
FEATURES = [c for c in df.columns if c not in ["Date", TARGET]]

TRAIN_YEARS = 5
test_years  = sorted(df["Date"].dt.year.unique())[TRAIN_YEARS:]

## Models to compare
models = {
    "Random Walk":  None,
    "CatBoost":     CatBoostRegressor(iterations=500, learning_rate=0.05, depth=6,
                                      loss_function="RMSE", random_seed=42, verbose=False),
    "XGBoost":      XGBRegressor(n_estimators=500, learning_rate=0.05, max_depth=6,
                                 random_state=42, verbosity=0),
    "LightGBM":     LGBMRegressor(n_estimators=500, learning_rate=0.05, max_depth=6,
                                  random_state=42, verbose=-1),
    "Random Forest":RandomForestRegressor(n_estimators=300, max_depth=6,
                                          random_state=42, n_jobs=-1),
    "Ridge":        Ridge(alpha=1.0),
    "Lasso":        Lasso(alpha=0.001, max_iter=5000),
}

def rmse(a, b): return np.sqrt(np.mean((a - b)**2))
def r2(a, b):
    ss_res = np.sum((a - b)**2)
    ss_tot = np.sum((a - np.mean(a))**2)
    return 1 - ss_res / ss_tot

results = {name: {"preds": [], "actuals": []} for name in models}

for test_year in test_years:
    train = df[df["Date"].dt.year < test_year].copy()
    test  = df[df["Date"].dt.year == test_year].copy()
    if len(test) == 0:
        continue

    X_train = train[FEATURES]
    y_train = train[TARGET]
    X_test  = test[FEATURES]
    y_test  = test[TARGET].values

    for name, model in models.items():

        if name == "Random Walk":
            preds = np.zeros(len(y_test))

        elif name in ("Ridge", "Lasso"):
            scaler  = StandardScaler()
            X_tr_sc = scaler.fit_transform(X_train.fillna(0))
            X_te_sc = scaler.transform(X_test.fillna(0))
            model.fit(X_tr_sc, y_train)
            preds = model.predict(X_te_sc)

        else:
            model.fit(X_train, y_train)
            preds = model.predict(X_test)

        results[name]["preds"].extend(preds)
        results[name]["actuals"].extend(y_test)

## Print summary
print(f"\n{'Model':<20} {'RMSE':>8}  {'R²':>8}")
print("─" * 42)
for name in models:
    a = np.array(results[name]["actuals"])
    p = np.array(results[name]["preds"])
    mask = ~np.isnan(p)
    print(f"{name:<20} {rmse(a[mask], p[mask]):>8.4f}  {r2(a[mask], p[mask]):>8.4f}")
