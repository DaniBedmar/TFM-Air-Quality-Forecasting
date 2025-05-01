import polars as pl
from utils.dictionaries import tramit_file_columns,tramit_file_index
def tramit_file_reader(file_name, cols_to_keep = tramit_file_columns):
    
    col_widths = tramit_file_index
    col_names = tramit_file_columns

    slice_tuples = []
    offset = 0

    for i, separation in enumerate(col_widths):
        start = offset
        end = separation
        slice_tuples.append((start, end,col_names[i]))
        offset += separation

    df = pl.read_csv(
        file_name,
        has_header=False,
        encoding='ISO-8859-1',
        truncate_ragged_lines=True,
        new_columns=["full_str"],
        skip_rows=1
    )

    exprs = [
        pl.col("full_str")
        .str.slice(start, end)
        .str.strip_chars()
        .alias(col_name)
        for start, end, col_name in slice_tuples
    ]

    df = df.with_columns(exprs).select(cols_to_keep)
    df = df.with_columns(pl.col("FEC_MATRICULA","FEC_TRAMITE","FEC_PRIM_MATRICULACION").str.to_date("%d%m%Y", strict=False))
    return df