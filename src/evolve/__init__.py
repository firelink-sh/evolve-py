from .__version__ import __version__  # noqa

from collections import defaultdict
from pathlib import Path
from typing import (
    Dict,
    List,
    Tuple,
)

import pandas as pd


def read_fwf(
    filepath: Path | str,
    colspecs: List[Tuple[int, int]],
    colnames: List[str],
    coltypes: Dict[str, str],
):
    data = defaultdict(list)
    with open(filepath, "r") as f:
        for line in f.readlines():
            for col, (st, ed) in zip(colnames, colspecs):
                data[col].append(line[st:(st + ed)])

    df = pd.DataFrame(data)
    df.astype(coltypes)
    return df

