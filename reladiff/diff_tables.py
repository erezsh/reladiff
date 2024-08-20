"""Provides classes for performing a table diff
"""

from abc import ABC, abstractmethod
from enum import Enum
from contextlib import contextmanager
from operator import methodcaller
from typing import Dict, Tuple, Iterator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from runtype import dataclass

from .info_tree import InfoTree, SegmentInfo

from .utils import safezip, getLogger, Vector
from .thread_utils import ThreadedYielder
from .table_segment import TableSegment, create_mesh_from_points, EmptyTable, EmptyTableSegment
from sqeleton.abcs import IKey

logger = getLogger(__name__)


class Algorithm(Enum):
    AUTO = "auto"
    JOINDIFF = "joindiff"
    HASHDIFF = "hashdiff"


DiffResult = Iterator[Tuple[str, tuple]]  # Iterator[Tuple[Literal["+", "-"], tuple]]


@dataclass
class ThreadBase:
    "Provides utility methods for optional threading"

    threaded: bool = True
    max_threadpool_size: Optional[int] = 1

    def _thread_map(self, func, iterable):
        if not self.threaded:
            return map(func, iterable)

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            return task_pool.map(func, iterable)

    def _threaded_call(self, func, iterable, **kw):
        "Calls a method for each object in iterable."
        return list(self._thread_map(methodcaller(func, **kw), iterable))

    def _thread_as_completed(self, func, iterable):
        if not self.threaded:
            yield from map(func, iterable)
            return

        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(func, item) for item in iterable]
            for future in as_completed(futures):
                yield future.result()

    def _threaded_call_as_completed(self, func, iterable):
        "Calls a method for each object in iterable. Returned in order of completion."
        return self._thread_as_completed(methodcaller(func), iterable)

    @contextmanager
    def _run_in_background(self, *funcs):
        with ThreadPoolExecutor(max_workers=self.max_threadpool_size) as task_pool:
            futures = [task_pool.submit(f) for f in funcs if f is not None]
            yield futures
            for f in futures:
                f.result()


@dataclass
class DiffStats:
    diff_by_sign: Dict[str, int]
    table1_count: int
    table2_count: int
    unchanged: int
    diff_percent: float


@dataclass
class DiffResultWrapper:
    diff: iter  # DiffResult
    info_tree: InfoTree
    stats: dict
    result_list: list = []

    def __iter__(self):
        yield from self.result_list
        for i in self.diff:
            self.result_list.append(i)
            yield i

    def _get_stats(self) -> DiffStats:
        list(self)  # Consume the iterator into result_list, if we haven't already

        diff_by_key = {}
        for sign, values in self.result_list:
            k = values[: len(self.info_tree.info.tables[0].key_columns)]
            if k in diff_by_key:
                assert sign != diff_by_key[k]
                diff_by_key[k] = "!"
            else:
                diff_by_key[k] = sign

        diff_by_sign = {k: 0 for k in "+-!"}
        for sign in diff_by_key.values():
            diff_by_sign[sign] += 1

        table1_count = self.info_tree.info.rowcounts[1]
        table2_count = self.info_tree.info.rowcounts[2]
        unchanged = table1_count - diff_by_sign["-"] - diff_by_sign["!"]
        diff_percent = 1 - unchanged / max(table1_count, table2_count, 1)

        return DiffStats(diff_by_sign, table1_count, table2_count, unchanged, diff_percent)

    def get_stats_string(self):
        diff_stats = self._get_stats()
        string_output = ""
        string_output += f"{diff_stats.table1_count} rows in table A\n"
        string_output += f"{diff_stats.table2_count} rows in table B\n"
        string_output += f"{diff_stats.diff_by_sign['-']} rows exclusive to table A (not present in B)\n"
        string_output += f"{diff_stats.diff_by_sign['+']} rows exclusive to table B (not present in A)\n"
        string_output += f"{diff_stats.diff_by_sign['!']} rows updated\n"
        string_output += f"{diff_stats.unchanged} rows unchanged\n"
        string_output += f"{100*diff_stats.diff_percent:.2f}% difference score\n"
        return string_output

    def get_stats_dict(self):
        diff_stats = self._get_stats()
        json_output = {
            "rows_A": diff_stats.table1_count,
            "rows_B": diff_stats.table2_count,
            "exclusive_A": diff_stats.diff_by_sign["-"],
            "exclusive_B": diff_stats.diff_by_sign["+"],
            "updated": diff_stats.diff_by_sign["!"],
            "unchanged": diff_stats.unchanged,
            "total": sum(diff_stats.diff_by_sign.values()),
            "stats": self.stats,
        }

        return json_output


@dataclass(frozen=True)
class TableDiffer(ThreadBase, ABC):
    bisection_factor = 32
    stats: dict = {}
    allow_empty_tables: bool = False

    def diff_tables(
        self, table1: TableSegment, table2: TableSegment, *, info_tree: InfoTree = None
    ) -> DiffResultWrapper:
        """Diff the given tables.

        Parameters:
            table1 (TableSegment): The "before" table to compare. Or: source table
            table2 (TableSegment): The "after" table to compare. Or: target table

        Returns:
            An iterator that yield pair-tuples, representing the diff. Items can be either -
            ('-', row) for items in table1 but not in table2.
            ('+', row) for items in table2 but not in table1.
            Where `row` is a tuple of values, corresponding to the diffed columns.
        """
        if info_tree is None:
            info_tree = InfoTree(SegmentInfo([table1, table2]))
        return DiffResultWrapper(self._diff_tables_wrapper(table1, table2, info_tree), info_tree, self.stats)

    def _diff_tables_wrapper(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> DiffResult:
        try:
            # Query and validate schema
            table1, table2 = self._threaded_call(
                "with_schema", [table1, table2], allow_empty_table=self.allow_empty_tables
            )
            self._validate_and_adjust_columns(table1, table2)

            yield from self._diff_tables_root(table1, table2, info_tree)
        finally:
            info_tree.aggregate_info()

    def _validate_and_adjust_columns(self, table1: TableSegment, table2: TableSegment) -> DiffResult:
        pass

    def _diff_tables_root(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree) -> DiffResult:
        return self._bisect_and_diff_tables(table1, table2, info_tree)

    @abstractmethod
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
        ...

    def _bisect_and_diff_tables(self, table1: TableSegment, table2: TableSegment, info_tree: InfoTree):
        if len(table1.key_columns) != len(table2.key_columns):
            raise ValueError("Tables should have an equivalent number of key columns!")

        key_types1 = table1.key_types
        key_types2 = table2.key_types
        is_empty1 = isinstance(table1, EmptyTableSegment)
        is_empty2 = isinstance(table2, EmptyTableSegment)

        for kt in ([] if is_empty1 else key_types1) + ([] if is_empty2 else key_types2):
            if not isinstance(kt, IKey):
                raise NotImplementedError(f"Cannot use a column of type {kt} as a key")

        if not (is_empty1 or is_empty2):
            for kt1, kt2 in safezip(key_types1, key_types2):
                if kt1.python_type is not kt2.python_type:
                    raise TypeError(f"Incompatible key types: {kt1} and {kt2}")

        # Query min/max values
        key_ranges = self._threaded_call_as_completed("query_key_range", [table1, table2])

        # Start with the first completed value, so we don't waste time waiting
        try:
            min_key1, max_key1 = self._parse_key_range_result(key_types1, next(key_ranges))
        except EmptyTable:
            if not self.allow_empty_tables:
                raise
            try:
                min_key1, max_key1 = self._parse_key_range_result(key_types2, next(key_ranges))
            except EmptyTable:
                # Both tables are empty
                info_tree.info.set_diff([])
                info_tree.info.max_rows = 0
                info_tree.info.rowcounts = {1:0, 2:0}
                return []

        btable1, btable2 = [t.new_key_bounds(min_key=min_key1, max_key=max_key1) for t in (table1, table2)]

        logger.info(
            f"Diffing segments at key-range: {min_key1}..{max_key1}. "
            f"size: table1 <= {btable1.approximate_size()}, table2 <= {btable2.approximate_size()}"
        )

        ti = ThreadedYielder(self.max_threadpool_size)
        # Bisect (split) the table into segments, and diff them recursively.
        ti.submit(self._bisect_and_diff_segments, ti, btable1, btable2, info_tree)

        # Now we check for the second min-max, to diff the portions we "missed".
        # This is achieved by subtracting the table ranges, and dividing the resulting space into aligned boxes.
        # For example, given tables A & B, and a 2D compound key, where A was queried first for key-range,
        # the regions of B we need to diff in this second pass are marked by B1..8:
        # ┌──┬──────┬──┐
        # │B1│  B2  │B3│
        # ├──┼──────┼──┤
        # │B4│  A   │B5│
        # ├──┼──────┼──┤
        # │B6│  B7  │B8│
        # └──┴──────┴──┘
        # Overall, the max number of new regions in this 2nd pass is 3^|k| - 1

        try:
            min_key2, max_key2 = self._parse_key_range_result(key_types1, next(key_ranges))
        except StopIteration:  # First table is empty
            return ti
        except EmptyTable:  # Second table is empty
            if not self.allow_empty_tables:
                raise
            return ti

        points = [list(sorted(p)) for p in safezip(min_key1, min_key2, max_key1, max_key2)]
        box_mesh = create_mesh_from_points(*points)

        new_regions = [(p1, p2) for p1, p2 in box_mesh if p1 < p2 and not (p1 >= min_key1 and p2 <= max_key1)]

        for p1, p2 in new_regions:
            extra_tables = [t.new_key_bounds(min_key=p1, max_key=p2) for t in (table1, table2)]
            ti.submit(self._bisect_and_diff_segments, ti, *extra_tables, info_tree)

        return ti

    def _parse_key_range_result(self, key_types, key_range) -> Tuple[Vector, Vector]:
        if isinstance(key_range, Exception):
            raise key_range

        min_key_values, max_key_values = key_range

        # We add 1 because our ranges are exclusive of the end (like in Python)
        try:
            min_key = Vector(key_type.make_value(mn) for key_type, mn in safezip(key_types, min_key_values))
            max_key = Vector(key_type.make_value(mx) + 1 for key_type, mx in safezip(key_types, max_key_values))
        except (TypeError, ValueError) as e:
            raise type(e)(f"Cannot apply {key_types} to '{min_key_values}', '{max_key_values}'.") from e

        return min_key, max_key

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

        # Choose evenly spaced checkpoints (according to min_key and max_key)
        biggest_table = max(table1, table2, key=methodcaller("approximate_size"))
        checkpoints = biggest_table.choose_checkpoints(self.bisection_factor - 1)

        # Create new instances of TableSegment between each checkpoint
        segmented1 = table1.segment_by_checkpoints(checkpoints)
        segmented2 = table2.segment_by_checkpoints(checkpoints)

        # Recursively compare each pair of corresponding segments between table1 and table2
        for i, (t1, t2) in enumerate(safezip(segmented1, segmented2)):
            info_node = info_tree.add_node(t1, t2, max_rows=max_rows)
            ti.submit(
                self._diff_segments, ti, t1, t2, info_node, max_rows, level + 1, i + 1, len(segmented1), priority=level
            )
