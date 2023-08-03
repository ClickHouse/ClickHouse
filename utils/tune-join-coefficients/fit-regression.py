import numpy as np
from sklearn.linear_model import LinearRegression
import json

with open("params.json", "r") as f:
    params = np.array(json.load(f))

with open("timings.txt", "r") as f:
    timings = []
    for line in f:
        timings.append(float(line))
    timings = np.array(timings)

assert (
    len(params) == len(timings) and len(timings) > 0
), "Got mismatching number of data points in params and timings"


timings *= 1_000_000  # seconds to microseconds

Ys = timings
Xs = params[: timings.shape[0]]

# params contains:
# [size_a, size_b, extra_cols_a, extra_cols_b, predicate_on_left_side, predicate_on_right_side]
size_a = Xs[:, 0]
size_b = Xs[:, 1]
extra_cols_a = Xs[:, 2]
extra_cols_b = Xs[:, 3]
pred_left = Xs[:, 4]
pred_right = Xs[:, 5]
cells_a = size_a * (1 + extra_cols_a)
cells_b = size_b * (1 + extra_cols_b)
postpred_rows_a = size_a - size_a * pred_left * 0.5
postpred_rows_b = size_b - size_b * pred_right * 0.5
postpred_cells_a = postpred_rows_a * (1 + extra_cols_a)
postpred_cells_b = postpred_rows_b * (1 + extra_cols_b)
outsize = np.minimum(size_a, size_b) * (1 + extra_cols_a + extra_cols_b)
outsize_rows = np.minimum(size_a, size_b)

size_and_cells_and_postpredsize = np.hstack(
    (
        size_a[:, None],
        size_b[:, None],
        cells_a[:, None],
        cells_b[:, None],
        postpred_rows_a[:, None],
        postpred_rows_b[:, None],
        postpred_cells_a[:, None],
        postpred_cells_b[:, None],
        outsize[:, None],
    )
)

model_lin = LinearRegression(positive=True).fit(size_and_cells_and_postpredsize, Ys)
print(
    f"R2 score of the model (on the training data): {model_lin.score(size_and_cells_and_postpredsize, Ys)}"
)
print(model_lin.coef_[0], "= coefficient for row count of left table")
print(model_lin.coef_[1], "= coefficient for row count of right table")
print(model_lin.coef_[2], "= coefficient for row_count*selected_columns of left table")
print(model_lin.coef_[3], "= coefficient for row_count*selected_columns of right table")
print(
    model_lin.coef_[4],
    "= coefficient for row_count after WHERE filtering of left table",
)
print(
    model_lin.coef_[5],
    "= coefficient for row_count after WHERE filtering of right table",
)
print(
    model_lin.coef_[6],
    "= coefficient for row_count*selected_columns after WHERE filtering of left table",
)
print(
    model_lin.coef_[7],
    "= coefficient for row_count*selected_columns after WHERE filtering of right table",
)
print(model_lin.coef_[8], "= coefficient for row_count in output")
