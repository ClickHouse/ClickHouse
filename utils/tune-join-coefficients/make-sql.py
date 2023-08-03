import json


def makeTable(size, outfile):
    # make a table with `size` rows. id is in order, c1,c2,c3 are random, but have same NDV as id
    outfile.write(
        f"CREATE TABLE IF NOT EXISTS N{size} (id UInt64, c1 UInt64, c2 UInt64, c3 UInt64) ENGINE=MergeTree ORDER BY id AS SELECT number, (number * 1103515245 + 12345) % pow(2, 31), (number * 1103515245 + 12345) % pow(2, 31), (number * 1103515245 + 12345) % pow(2, 31) FROM numbers({size});\n"
    )


def timeJoin(size_a, size_b, extra_cols_a, extra_cols_b, outfile, pk=False):
    cols = ["id", "c1", "c2", "c3"]
    if pk:
        oncol = cols.pop(0)
    else:
        oncol = cols.pop(1)
    t1 = f"N{size_a}"
    t2 = f"N{size_b}"
    sel_cols = ",".join(
        [f"{t1}.{c}" for c in cols[:extra_cols_a]]
        + [f"{t2}.{c}" for c in cols[:extra_cols_b]]
    )
    outfile.write(
        f"SELECT count(), (* APPLY sum) FROM (SELECT {sel_cols} FROM {t1} INNER JOIN {t2} ON {t1}.{oncol} = {t2}.{oncol});\n"
    )


def timeJoinFilter(size_a, size_b, extra_cols_a, extra_cols_b, filter_left, outfile):
    cols = ["id", "c1", "c2", "c3"]
    oncol = cols.pop(0)
    t1 = f"N{size_a}"
    t2 = f"N{size_b}"
    sel_cols = ",".join(
        [f"{t1}.{c}" for c in cols[:extra_cols_a]]
        + [f"{t2}.{c}" for c in cols[:extra_cols_b]]
    )
    pred_threshold = int((2**31) / 2)
    pred_table = t1 if filter_left else t2
    predicate = f"WHERE {pred_table}.c1 < {pred_threshold}"
    outfile.write(
        f"SELECT count(), (* APPLY sum) FROM (SELECT {sel_cols} FROM {t1} INNER JOIN {t2} ON {t1}.{oncol} = {t2}.{oncol} {predicate});\n"
    )


start = 1_000
stop = 100_000_000
sizes = [
    int(start + n * ((stop - start) / 9)) for n in range(10)
]  # 1K to 100M, increments of ~11M
with open("create_tables.sql", "w") as outfile:
    for size in sizes:
        makeTable(size, outfile)

params = []
with open("queries.sql", "w") as outfile:
    for extra_cols_a in range(3):  # num selected cols from left
        for extra_cols_b in range(3):  # num selected cols from right
            for size_a in sizes[::2]:  # num rows in left
                for size_b in sizes[::2]:  # num rows in right
                    # for pk in [True, False]: #joing on pk vs non-pk seems to have no effect
                    if size_a != size_b and (extra_cols_a + extra_cols_b) > 0:
                        timeJoin(size_a, size_b, extra_cols_a, extra_cols_b, outfile)
                        params.append(
                            [size_a, size_b, extra_cols_a, extra_cols_b, 0, 0]
                        )

    for extra_cols_a in range(1, 3):
        for extra_cols_b in range(1, 3):
            for size_a in sizes[::2]:
                for size_b in sizes[::2]:
                    if size_a != size_b:
                        timeJoinFilter(
                            size_a, size_b, extra_cols_a, extra_cols_b, True, outfile
                        )
                        params.append(
                            [size_a, size_b, extra_cols_a, extra_cols_b, 1, 0]
                        )
                        timeJoinFilter(
                            size_b, size_a, extra_cols_b, extra_cols_a, False, outfile
                        )
                        params.append(
                            [size_b, size_a, extra_cols_b, extra_cols_a, 0, 1]
                        )


with open("params.json", "w") as outfile:
    json.dump(params, outfile)

print(
    "Created sql files for queries to be run, and a json file for the parameters of each select query."
)
