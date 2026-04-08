##
#  Basic CatBoost model on Factors.csv
#  Walk-forward cross-validation with feature importance
##

import pandas as pd
import numpy as np
from catboost import CatBoostRegressor, Pool
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt

## Load data
df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])
df = df.dropna(subset=["Salmon (NOK/KG)"]).reset_index(drop=True)

TARGET  = "Salmon (NOK/KG)"
FEATURES = [c for c in df.columns if c not in ["Date", TARGET]]

## Walk-forward cross-validation
#  Train on first N years, test on next 1 year, roll forward
TRAIN_YEARS = 5   # minimum initial training window
results     = []

test_dates  = df["Date"].dt.year.unique()
test_dates  = sorted(test_dates)[TRAIN_YEARS:]

all_preds = []
all_actuals = []
all_dates = []

for test_year in test_dates:

    train = df[df["Date"].dt.year < test_year].copy()
    test  = df[df["Date"].dt.year == test_year].copy()

    if len(test) == 0:
        continue

    X_train = train[FEATURES]
    y_train = train[TARGET]
    X_test  = test[FEATURES]
    y_test  = test[TARGET]

    model = CatBoostRegressor(
        iterations    = 500,
        learning_rate = 0.05,
        depth         = 6,
        loss_function = "RMSE",
        random_seed   = 42,
        verbose       = False
    )

    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    all_preds.extend(preds)
    all_actuals.extend(y_test.values)
    all_dates.extend(test["Date"].values)

    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2   = r2_score(y_test, preds)
    print(f"  {test_year}  RMSE={rmse:.4f}  R²={r2:.4f}  (n={len(test)})")

## Overall metrics
all_preds   = np.array(all_preds)
all_actuals = np.array(all_actuals)
print(f"\nOverall  RMSE={np.sqrt(mean_squared_error(all_actuals, all_preds)):.4f}"
      f"  R²={r2_score(all_actuals, all_preds):.4f}")

## Train final model on all data for feature importance
X_all = df[FEATURES]
y_all = df[TARGET]

final_model = CatBoostRegressor(
    iterations    = 500,
    learning_rate = 0.05,
    depth         = 6,
    loss_function = "RMSE",
    random_seed   = 42,
    verbose       = False
)
final_model.fit(X_all, y_all)

## Feature importance
importance = pd.Series(
    final_model.get_feature_importance(),
    index=FEATURES
).sort_values(ascending=False)

print("\nTop 20 features:")
print(importance.head(20).to_string())

## Plot predictions vs actuals
fig, axes = plt.subplots(2, 1, figsize=(14, 8))

axes[0].plot(all_dates, all_actuals, label="Actual", alpha=0.8)
axes[0].plot(all_dates, all_preds,   label="Predicted", alpha=0.8)
axes[0].set_title("Walk-forward predictions vs actuals")
axes[0].legend()
axes[0].grid(True, alpha=0.3)

importance.head(20).sort_values().plot(kind="barh", ax=axes[1])
axes[1].set_title("Top 20 feature importances (full sample)")
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()
