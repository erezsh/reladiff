import os
from functools import cmp_to_key
from numbers import Number
import logging
from typing import Iterator, Sequence, Tuple
from operator import attrgetter
from collections import Counter
from itertools import chain

from dataclasses import dataclass, field

# from runtype import dataclass     # TODO fix in runtype

from sqeleton.abcs import ColType_UUID, NumericType, PrecisionType, StringType, Boolean

from .info_tree import InfoTree
from .utils import safezip
from .thread_utils import ThreadedYielder
from .table_segment import TableSegment, EmptyTableSegment

from .diff_tables import TableDiffer

BENCHMARK = os.environ.get("BENCHMARK", False)

DEFAULT_BISECTION_THRESHOLD = 1024 * 16
DEFAULT_BISECTION_FACTOR = 32

logger = logging.getLogger("hashdiff_tables")


def compare_element(a, b):
    """Compare a and b, treat None as the smallest value.

    Return -1 if a < b, 0 if a == b and 1 if a > b.
    """
    if a == b:
        return 0
    if b is not None and ((a is None) or (a < b)):
        return -1
    return 1


def compare(a: Tuple[str, Sequence], b: Tuple[str, Sequence]) -> int:
    """Compare two sequences of the same length.

    Compare a and b until the first element a[1][i] differs from b[1][i].
    See compare_element() for detailed comparison rules.

    Return -1 if a < b, 0 if a == b and 1 if a > b.
    """
    for i in range(len(a[1])):
        res = compare_element(a[1][i], b[1][i])
        if res != 0:
            return res
    return 0


def diff_sets(a: list, b: list, skip_sort_results: bool, duplicate_rows_support: bool) -> Iterator:
    if duplicate_rows_support:
        c = Counter(b)
        c.subtract(a)
        diff = (("+", k) if count > 0 else ("-", k) for k, count in c.items() for _ in range(abs(count)))
    else:
        sa = set(a)
        sb = set(b)
        diff = chain((("-", x) for x in sa - sb), (("+", x) for x in sb - sa))
    return diff if skip_sort_results else sorted(diff, key=cmp_to_key(compare))  # sort by key


@dataclass(frozen=True)
class HashDiffer(TableDiffer):
    """Finds the diff between two SQL tables

    The algorithm uses hashing to quickly check if the tables are different, and then applies a
    bisection search recursively to find the differences efficiently.

    Works best for comparing tables that are mostly the same, with minor discrepancies.

    Parameters:
        bisection_factor (int): Into how many segments to bisect per iteration.
        bisection_threshold (Number): When should we stop bisecting and compare locally (in row count).
        threaded (bool): Enable/disable threaded diffing. Needed to take advantage of database threads.
        max_threadpool_size (int): Maximum size of each threadpool. ``None`` means auto.
                                   Only relevant when `threaded` is ``True``.
                                   There may be many pools, so number of actual threads can be a lot higher.
        skip_sort_results (bool): Skip sorting the hashdiff output by key for better performance.
                                  Entries with the same key but different column values may not appear adjacent in the output.
        duplicate_rows_support (bool): If ``True``, the algorithm will support duplicate rows in the tables.
    """

    bisection_factor: int = DEFAULT_BISECTION_FACTOR
    bisection_threshold: Number = DEFAULT_BISECTION_THRESHOLD  # Accepts inf for tests
    skip_sort_results: bool = False
    duplicate_rows_support: bool = True

    stats: dict = field(default_factory=dict)

    def __post_init__(self):
        # Validate options
        if self.bisection_factor >= self.bisection_threshold:
            raise ValueError("Incorrect param values (bisection factor must be lower than threshold)")
        if self.bisection_factor < 2:
            raise ValueError("Must have at least two segments per iteration (i.e. bisection_factor >= 2)")

    def _validate_and_adjust_columns(self, table1, table2):
        if isinstance(table1, EmptyTableSegment) or isinstance(table2, EmptyTableSegment):
            # Skip all logic; it only pertains to column mismatch
            return

        for c1, c2 in safezip(table1.relevant_columns, table2.relevant_columns):
            if c1 not in table1._schema:
                raise ValueError(f"Column '{c1}' not found in schema for table {table1}")
            if c2 not in table2._schema:
                raise ValueError(f"Column '{c2}' not found in schema for table {table2}")

            # Update schemas to minimal mutual precision
            col1 = table1._schema[c1]
            col2 = table2._schema[c2]
            if isinstance(col1, PrecisionType):
                if not isinstance(col2, PrecisionType):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

                lowest = min(col1, col2, key=attrgetter("precision"))

                if col1.precision != col2.precision:
                    logger.warning(f"Using reduced precision {lowest} for column '{c1}'. Types={col1}, {col2}")

                table1._schema[c1] = col1.replace(precision=lowest.precision, rounds=lowest.rounds)
                table2._schema[c2] = col2.replace(precision=lowest.precision, rounds=lowest.rounds)

            elif isinstance(col1, (NumericType, Boolean)):
                if not isinstance(col2, (NumericType, Boolean)):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

                lowest = min(col1, col2, key=attrgetter("precision"))

                if col1.precision != col2.precision:
                    logger.warning(f"Using reduced precision {lowest} for column '{c1}'. Types={col1}, {col2}")

                if lowest.precision != col1.precision:
                    table1._schema[c1] = col1.replace(precision=lowest.precision)
                if lowest.precision != col2.precision:
                    table2._schema[c2] = col2.replace(precision=lowest.precision)

            elif isinstance(col1, ColType_UUID):
                if not isinstance(col2, ColType_UUID):
                    if isinstance(col1, StringType) and isinstance(col2, StringType):
                        pass  # Allow String_UUID to be compared to Text.
                    else:
                        raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

            elif isinstance(col1, StringType):
                if not isinstance(col2, StringType):
                    raise TypeError(f"Incompatible types for column '{c1}':  {col1} <-> {col2}")

        for t in [table1, table2]:
            for c in t.relevant_columns:
                ctype = t._schema[c]
                if not ctype.supported:
                    logger.warning(
                        f"[{t.database.name}] Column '{c}' of type '{ctype}' has no compatibility handling. "
                        "If encoding/formatting differs between databases, it may result in false positives."
                    )

    def _diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        max_rows: int,
        level=0,
        segment_index=None,
        segment_count=None,
    ):
        logger.info(
            ". " * level + f"Diffing segment {segment_index}/{segment_count}, "
            f"key-range: {table1.min_key}..{table2.max_key}, "
            f"size <= {max_rows}"
        )

        # When benchmarking, we want the ability to skip checksumming. This
        # allows us to download all rows for comparison in performance. By
        # default, reladiff will checksum the section first (when it's below
        # the threshold) and _then_ download it.
        if BENCHMARK:
            if max_rows < self.bisection_threshold:
                return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level=level, max_rows=max_rows)

        if isinstance(table1, EmptyTableSegment) or isinstance(table1, EmptyTableSegment):
            # Optimization: No need to checksum if one of the tables is empty
            count1, count2 = self._threaded_call("count", [table1, table2])
            checksum1 = checksum2 = None
        else:
            (count1, checksum1), (count2, checksum2) = self._threaded_call("count_and_checksum", [table1, table2])

        assert not info_tree.info.rowcounts
        info_tree.info.rowcounts = {1: count1, 2: count2}

        if count1 == 0 and count2 == 0:
            logger.debug(
                "Uneven distribution of keys detected in segment %s..%s (big gaps in the key column). "
                "For better performance, we recommend to increase the bisection-threshold.",
                table1.min_key,
                table1.max_key,
            )
            assert checksum1 is None and checksum2 is None
            info_tree.info.is_diff = False
            return

        if checksum1 == checksum2 and count1 == count2:
            info_tree.info.is_diff = False
            return

        info_tree.info.is_diff = True
        return self._bisect_and_diff_segments(ti, table1, table2, info_tree, level=level, max_rows=max(count1, count2))

    def _bisect_and_diff_segments(
        self,
        ti: ThreadedYielder,
        table1: TableSegment,
        table2: TableSegment,
        info_tree: InfoTree,
        level=0,
        max_rows=None,
    ):
        assert table1.is_bounded and table2.is_bounded

        max_space_size = max(table1.approximate_size(), table2.approximate_size())
        if max_rows is None:
            # We can be sure that row_count <= max_rows iff the table key is unique
            max_rows = max_space_size
            info_tree.info.max_rows = max_rows

        # If count is below the threshold, just download and compare the columns locally
        # This saves time, as bisection speed is limited by ping and query performance.
        if max_rows < self.bisection_threshold or max_space_size < self.bisection_factor * 2:
            rows1, rows2 = self._threaded_call("get_values", [table1, table2])
            diff = list(diff_sets(rows1, rows2, self.skip_sort_results, self.duplicate_rows_support))

            info_tree.info.set_diff(diff)
            info_tree.info.rowcounts = {1: len(rows1), 2: len(rows2)}

            logger.info(". " * level + f"Diff found {len(diff)} different rows.")
            self.stats["rows_downloaded"] = self.stats.get("rows_downloaded", 0) + max(len(rows1), len(rows2))
            return diff

        return super()._bisect_and_diff_segments(ti, table1, table2, info_tree, level, max_rows)
