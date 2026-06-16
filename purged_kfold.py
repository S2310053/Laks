# This module constructs the purged k-fold cross-validation splits (Lopez de Prado, 2018)
#
##
# This is the ADDITIVE "true" purged k-fold method, kept separate from the existing
# walk-forward CV (which lives unchanged inside each model script). It is run over the
# FULL sample (from the first forward-price week) so that every observation is used as a
# test point exactly once, then predictions are POOLED across folds for one out-of-sample
# performance estimate per model/horizon, plus per-fold breakdowns.
#
# Unlike walk-forward, the training set is NOT required to come before the test fold:
# for a middle test fold the training data sits on BOTH sides in time. Leakage is removed
# locally around each test fold by purging (and a feature-lookback buffer), not by global
# chronology:
#   * LEFT purge  = h weeks   — training labels (forward returns of length h) that would
#                               reach into the test fold.
#   * RIGHT purge = max(h, L) — training feature windows (backward HAR/vol lookback of
#                               length L = 52w) that would reach back into the test fold.
# Boundary folds are one-sided automatically (fold 1 has no training to its left; the last
# fold has none to its right).
#
# Folds are defined ONCE on a per-horizon date grid (dates with a valid target, from
# PKF_START) and shared by every model, so all models use identical fold date-ranges and
# remain directly comparable (and DM-vs-OLS aligns on common dates).
##

import numpy as np
import pandas as pd

# First forward-price week in Factors.csv — the agreed start of the purged-CV sample.
# (Forward contracts first quote here; OLS needs them, and all models share this start so
#  their folds align in time.)
PKF_START = "2006-06-14"

# Number of folds for the purged k-fold method (4 → each fold trains on ~3/4 of the data).
N_FOLDS = 4

# Longest backward feature lookback in the feature set (∆ Salmon 12m and RVol 52w both use
# rolling(52)). Drives the conservative right-side purge so no training feature window
# overlaps the test fold.
FEATURE_LOOKBACK_WEEKS = 52

# Forward-label horizon h (weeks) per target horizon — equals the existing purge_weeks.
H_WEEKS = {"0w": 0, "1w": 1, "2w": 2, "1m": 4, "3m": 13, "6m": 26, "12m": 52}


def purge_weeks_for(horizon):
    """Return (left_purge, right_purge) in weeks for a horizon label."""
    h = H_WEEKS[horizon]
    return h, max(h, FEATURE_LOOKBACK_WEEKS)


def make_fold_specs(master_dates, n_folds=N_FOLDS, left_purge_weeks=0, right_purge_weeks=0):
    """Build fold date-ranges + purge cut-offs from a per-horizon master date grid.

    master_dates : 1-D array/Series of the testable weekly dates (target valid, >= PKF_START).
    Returns a list of dicts, one per fold, with:
        fold, test_start, test_end (inclusive test date range),
        left_cut  (purge everything in [left_cut, test_start) out of training),
        right_cut (purge everything in (test_end, right_cut] out of training).
    Test folds are contiguous equal-size blocks (by position) of the master grid, so their
    union is the whole grid with no overlap.
    """
    d = (pd.to_datetime(pd.Series(master_dates))
           .drop_duplicates().sort_values().reset_index(drop=True))
    n = len(d)
    if n < n_folds:
        raise ValueError(f"Not enough dates ({n}) for {n_folds} folds")
    fold_size = n // n_folds
    left_td  = pd.Timedelta(weeks=int(left_purge_weeks))
    right_td = pd.Timedelta(weeks=int(right_purge_weeks))

    specs = []
    for k in range(n_folds):
        ts = k * fold_size
        te = (k + 1) * fold_size if k < n_folds - 1 else n  # last fold absorbs remainder
        test_start = d.iloc[ts]
        test_end   = d.iloc[te - 1]
        specs.append({
            "fold":       k + 1,
            "test_start": test_start,
            "test_end":   test_end,
            "left_cut":   test_start - left_td,
            "right_cut":  test_end + right_td,
        })
    return specs


def split_by_dates(model_dates, spec):
    """Assign a model's rows to train/test for one fold, purely by date.

    Returns (train_mask, test_mask) as boolean numpy arrays aligned to model_dates.
    A row is TEST if its date is in [test_start, test_end]; TRAIN if it is strictly before
    left_cut or strictly after right_cut. Rows inside the purge zones are in neither.
    """
    md = pd.to_datetime(pd.Series(model_dates).reset_index(drop=True))
    test_mask  = (md >= spec["test_start"]) & (md <= spec["test_end"])
    train_mask = (md < spec["left_cut"]) | (md > spec["right_cut"])
    # A row can never be both (test range lies strictly inside [left_cut, right_cut]).
    assert not (test_mask & train_mask).any(), "fold overlap: train and test share a row"
    return train_mask.to_numpy(), test_mask.to_numpy()


def pool_fold_results(fold_results):
    """Concatenate per-fold OOS predictions into one date-sorted frame.

    fold_results : list of dicts each with arrays 'dates', 'actuals', 'preds' and scalar 'fold'.
    Returns a DataFrame(Date, Actual, Predicted, Fold) sorted by Date. Asserts that no date
    appears twice (every observation is tested at most once across folds).
    """
    frames = []
    for fr in fold_results:
        if len(fr["dates"]) == 0:
            continue
        frames.append(pd.DataFrame({
            "Date":      pd.to_datetime(fr["dates"]),
            "Actual":    np.asarray(fr["actuals"], dtype=float),
            "Predicted": np.asarray(fr["preds"], dtype=float),
            "Fold":      fr["fold"],
        }))
    if not frames:
        return pd.DataFrame(columns=["Date", "Actual", "Predicted", "Fold"])
    pooled = pd.concat(frames, ignore_index=True).sort_values("Date").reset_index(drop=True)
    assert pooled["Date"].is_unique, "pooled OOS has duplicate dates (folds overlap)"
    return pooled


# Self-test: validates fold geometry on the real data without fitting any model.
if __name__ == "__main__":
    df = pd.read_csv("Data/Factors.csv", parse_dates=["Date"])

    def _check(horizon):
        target = next(c for c in df.columns if c.startswith(f"Y {horizon} "))
        left, right = purge_weeks_for(horizon)
        master = df[(df["Date"] >= PKF_START) & df[target].notna()]["Date"]
        specs = make_fold_specs(master, N_FOLDS, left, right)

        print(f"\n=== horizon {horizon}  (left purge={left}w, right purge={right}w) ===")
        all_test_dates, all_train_counts = [], []
        for s in specs:
            tr_m, te_m = split_by_dates(master, s)
            md = pd.to_datetime(pd.Series(master).reset_index(drop=True))
            test_dates  = md[te_m]
            train_dates = md[tr_m]
            all_test_dates.append(test_dates)

            # 1. no training row inside [left_cut, right_cut]
            leak = train_dates[(train_dates >= s["left_cut"]) & (train_dates <= s["right_cut"])]
            assert len(leak) == 0, f"LEAK in fold {s['fold']}: {len(leak)} train rows in purge/test span"

            # 2. measured purge gaps match the configured widths (in weeks), where a
            #    neighbouring training block exists
            left_block  = train_dates[train_dates < s["test_start"]]
            right_block = train_dates[train_dates > s["test_end"]]
            gap_left  = (s["test_start"] - left_block.max()).days / 7 if len(left_block) else None
            gap_right = (right_block.min() - s["test_end"]).days / 7 if len(right_block) else None
            side = ("both" if len(left_block) and len(right_block)
                    else "right-only" if len(right_block) else "left-only")
            print(f"  fold {s['fold']}: test {s['test_start'].date()}→{s['test_end'].date()} "
                  f"| n_train={tr_m.sum():4d} n_test={te_m.sum():3d} | {side:10s} "
                  f"| gap_left={gap_left if gap_left is None else round(gap_left,1)}w "
                  f"gap_right={gap_right if gap_right is None else round(gap_right,1)}w")
            if gap_left is not None:
                assert gap_left >= left,  f"left gap {gap_left}w < {left}w in fold {s['fold']}"
            if gap_right is not None:
                assert gap_right >= right, f"right gap {gap_right}w < {right}w in fold {s['fold']}"

            all_train_counts.append(int(tr_m.sum()))

        # 3. test folds partition the master grid exactly (every date tested once)
        union = pd.concat(all_test_dates).sort_values().reset_index(drop=True)
        master_sorted = pd.to_datetime(pd.Series(master)).drop_duplicates().sort_values().reset_index(drop=True)
        assert union.equals(master_sorted), "test folds do not partition the sample"
        # 4. boundary folds are one-sided
        print(f"  -> partition OK, every one of {len(master_sorted)} dates tested exactly once")

    for hz in ["1w", "1m", "12m"]:
        _check(hz)
    print("\nALL SELF-TESTS PASSED")
