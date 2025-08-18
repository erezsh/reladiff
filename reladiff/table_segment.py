import time
from typing import List, Tuple, Dict
import logging
from itertools import product

from runtype import dataclass
from dataclasses import field

from .utils import safezip, Vector
from sqeleton.utils import ArithString, split_space
from sqeleton.databases import Database, DbPath, DbKey, DbTime
from sqeleton.abcs.database_types import String_UUID
from sqeleton.schema import Schema, create_schema
from sqeleton.queries import Count, Checksum, SKIP, table, this, Expr, min_, max_, Code
from sqeleton.queries.ast_classes import BinBoolOp
from sqeleton.queries.extras import ApplyFuncAndNormalizeAsString, NormalizeAsString

logger = logging.getLogger("table_segment")

RECOMMENDED_CHECKSUM_DURATION = 20


class EmptyTable(ValueError):
    pass


def split_key_space(min_key: DbKey, max_key: DbKey, count: int) -> List[DbKey]:
    assert min_key < max_key

    if max_key - min_key <= count:
        count = 1

    if isinstance(min_key, ArithString):
        assert type(min_key) is type(max_key)
        checkpoints = min_key.range(max_key, count)
    else:
        checkpoints = split_space(min_key, max_key, count)

    assert all(min_key < x < max_key for x in checkpoints)
    return [min_key] + checkpoints + [max_key]


def int_product(nums: List[int]) -> int:
    p = 1
    for n in nums:
        p *= n
    return p


def split_compound_key_space(mn: Vector, mx: Vector, count: int) -> List[List[DbKey]]:
    """Returns a list of split-points for each key dimension, essentially returning an N-dimensional grid of split points."""
    return [split_key_space(mn_k, mx_k, count) for mn_k, mx_k in safezip(mn, mx)]


def create_mesh_from_points(*values_per_dim: list) -> List[Tuple[Vector, Vector]]:
    """Given a list of values along each axis of N dimensional space,
    return an array of boxes whose start-points & end-points align with the given values,
    and together consitute a mesh filling that space entirely (within the bounds of the given values).

    Assumes given values are already ordered ascending.

    len(boxes) == ∏i( len(i)-1 )

    Example:
        ::
            >>> d1 = 'a', 'b', 'c'
            >>> d2 = 1, 2, 3
            >>> d3 = 'X', 'Y'
            >>> create_mesh_from_points(d1, d2, d3)
            [
                [('a', 1, 'X'), ('b', 2, 'Y')],
                [('a', 2, 'X'), ('b', 3, 'Y')],
                [('b', 1, 'X'), ('c', 2, 'Y')],
                [('b', 2, 'X'), ('c', 3, 'Y')]
            ]
    """
    assert all(len(v) >= 2 for v in values_per_dim), values_per_dim

    # Create tuples of (v1, v2) for each pair of adjacent values
    ranges = [list(zip(values[:-1], values[1:])) for values in values_per_dim]

    assert all(a <= b for r in ranges for a, b in r)

    # Create a product of all the ranges
    res = [tuple(Vector(a) for a in safezip(*r)) for r in product(*ranges)]

    expected_len = int_product(len(v) - 1 for v in values_per_dim)
    assert len(res) == expected_len, (len(res), expected_len)
    return res


@dataclass
class TableSegment:
    """Signifies a segment of rows (and selected columns) within a table

    Parameters:
        database (Database): Database instance. See :meth:`connect`
        table_path (:data:`DbPath`): Path to table in form of a tuple. e.g. `('my_dataset', 'table_name')`
        key_columns (Tuple[str]): Name of the key column, which uniquely identifies each row (usually id)
        update_column (str, optional): Name of updated column, which signals that rows changed.
                                       Usually updated_at or last_update. Used by `min_update` and `max_update`.
        extra_columns (Tuple[str, ...], optional): Extra columns to compare
        transform_columns (Dict[str, str], optional): A dictionary mapping column names to SQL transformation expressions.
                                                      These expressions are applied directly to the specified columns within the
                                                      comparison query, *before* the data is hashed or compared. Useful for
                                                      on-the-fly normalization (e.g., type casting, timezone conversions) without
                                                      requiring intermediate views or staging tables. Defaults to an empty dict.
        min_key (:data:`Vector`, optional): Lowest key value, used to restrict the segment
        max_key (:data:`Vector`, optional): Highest key value, used to restrict the segment
        min_update (:data:`DbTime`, optional): Lowest update_column value, used to restrict the segment
        max_update (:data:`DbTime`, optional): Highest update_column value, used to restrict the segment
        where (str, optional): An additional 'where' expression to restrict the search space.

        case_sensitive (bool): If false, the case of column names will adjust according to the schema. Default is true.

    """

    # Location of table
    database: Database
    table_path: DbPath

    # Columns
    key_columns: Tuple[str, ...]
    update_column: str = None
    extra_columns: Tuple[str, ...] = ()
    transform_columns: Dict[str, str] = field(default_factory=dict)

    # Restrict the segment
    min_key: Vector = None
    max_key: Vector = None
    min_update: DbTime = None
    max_update: DbTime = None
    where: str = None

    case_sensitive: bool = True
    _schema: Schema = None

    def __post_init__(self):
        if not self.update_column and (self.min_update or self.max_update):
            raise ValueError("Error: the min_update/max_update feature requires 'update_column' to be set.")

        if self.min_key is not None and self.max_key is not None and self.min_key >= self.max_key:
            raise ValueError(f"Error: min_key expected to be smaller than max_key! ({self.min_key} >= {self.max_key})")

        if self.min_update is not None and self.max_update is not None and self.min_update >= self.max_update:
            raise ValueError(
                f"Error: min_update expected to be smaller than max_update! ({self.min_update} >= {self.max_update})"
            )

    def _where(self):
        return f"({self.where})" if self.where else None

    def _with_raw_schema(self, raw_schema: dict, refine: bool = True, allow_empty_table=False) -> "TableSegment":
        # TODO validate all relevant columns are in the schema?
        cols = {c.lower() for c in self.relevant_columns}
        # We use v[0] to get the actual name (with correct case)
        raw_schema = {v[0]: v for k, v in raw_schema.items() if k.lower() in cols}
        schema, samples = self.database.process_query_table_schema(
            self.table_path, raw_schema, refine=refine, refine_where=self._where()
        )
        assert refine or samples is None
        is_empty_table = samples is not None and not samples
        if is_empty_table and not allow_empty_table:
            raise EmptyTable(f"Table {self.table_path} is empty. Use --allow-empty-tables to disable this protection.", self)

        res = self.new(_schema=create_schema(self.database, self.table_path, schema, self.case_sensitive), transform_columns = self.transform_columns)

        return EmptyTableSegment(res) if is_empty_table else res

    def with_schema(self, refine: bool = True, allow_empty_table: bool = False) -> "TableSegment":
        "Queries the table schema from the database, and returns a new instance of TableSegment, with a schema."
        if self._schema:
            return self

        return self._with_raw_schema(
            self.database.query_table_schema(self.table_path), refine=refine, allow_empty_table=allow_empty_table
        )

    def _cast_col_value(self, col, value):
        """Cast the value to the right type, based on the type of the column

        Currently only used to support native vs string UUID values.
        """
        assert self._schema
        t = self._schema[col]
        if isinstance(t, String_UUID):
            return str(value)
        return value

    def _get_column_transforms(self, col_name: str, aliased_col=None) -> Expr:
        """Get the Column Expression from the Transform Rules, if the column is present
        For hashdiff - aliased_col will be None
        For joindiff - aliased_col will be the aliased column name
        """
        transform_expr = self.transform_columns.get(col_name)

        if aliased_col:
            return Code(transform_expr.replace(col_name, aliased_col)) if transform_expr else None

        return Code(transform_expr) if transform_expr else this[col_name]

    def _make_key_range(self):
        if self.min_key is not None:
            for mn, k in safezip(self.min_key, self.key_columns):
                mn = self._cast_col_value(k, mn)
                yield BinBoolOp(">=", [self._get_column_transforms(k), mn])
        if self.max_key is not None:
            for k, mx in safezip(self.key_columns, self.max_key):
                mx = self._cast_col_value(k, mx)
                yield BinBoolOp("<", [self._get_column_transforms(k), mx])

    def _make_update_range(self):
        if self.min_update is not None:
            yield self.min_update <= this[self.update_column]
        if self.max_update is not None:
            yield this[self.update_column] < self.max_update

    @property
    def source_table(self):
        return table(*self.table_path, schema=self._schema)

    def make_select(self):
        return self.source_table.where(
            *self._make_key_range(), *self._make_update_range(), Code(self._where()) if self.where else SKIP
        )

    def get_values(self) -> list:
        "Download all the relevant values of the segment from the database"
        select = self.make_select().select(*self._relevant_columns_repr)
        return self.database.query(select, List[Tuple])

    def choose_checkpoints(self, count: int) -> List[List[DbKey]]:
        "Suggests a bunch of evenly-spaced checkpoints to split by, including start, end."

        assert self.is_bounded

        # Take Nth root of count, to approximate the appropriate box size
        count = int(count ** (1 / len(self.key_columns))) or 1

        return split_compound_key_space(self.min_key, self.max_key, count)

    def segment_by_checkpoints(self, checkpoints: List[List[DbKey]]) -> List["TableSegment"]:
        "Split the current TableSegment to a bunch of smaller ones, separated by the given checkpoints"

        return [self.new_key_bounds(min_key=s, max_key=e) for s, e in create_mesh_from_points(*checkpoints)]

    def new(self, **kwargs) -> "TableSegment":
        """Creates a copy of the instance using 'replace()'"""
        return self.replace(**kwargs)

    def new_key_bounds(self, min_key: Vector, max_key: Vector) -> "TableSegment":
        if self.min_key is not None:
            assert self.min_key <= min_key, (self.min_key, min_key)
            assert self.min_key < max_key

        if self.max_key is not None:
            assert min_key < self.max_key
            assert max_key <= self.max_key

        return self.replace(min_key=min_key, max_key=max_key)

    @property
    def relevant_columns(self) -> List[str]:
        extras = list(self.extra_columns)

        if self.update_column and self.update_column not in extras:
            extras = [self.update_column] + extras

        return list(self.key_columns) + extras

    @property
    def _relevant_columns_repr(self) -> List[Expr]:
        expressions = []
        for c in self.relevant_columns:
            expressions.append(NormalizeAsString(self._get_column_transforms(c), self._schema[c]))
        return expressions

    def count(self) -> int:
        """Count how many rows are in the segment, in one pass."""
        return self.database.query(self.make_select().select(Count()), int)

    def count_and_checksum(self) -> Tuple[int, int]:
        """Count and checksum the rows in the segment, in one pass."""
        start = time.monotonic()
        q = self.make_select().select(Count(), Checksum(self._relevant_columns_repr))
        count, checksum = self.database.query(q, tuple)
        duration = time.monotonic() - start
        if duration > RECOMMENDED_CHECKSUM_DURATION:
            logger.warning(
                "Checksum is taking longer than expected (%.2f). "
                "We recommend increasing --bisection-factor or decreasing --threads.",
                duration,
            )

        if count:
            assert checksum, (count, checksum)
        return count or 0, int(checksum) if count else None

    def query_key_range(self) -> Tuple[tuple, tuple]:
        """Query database for minimum and maximum key. This is used for setting the initial bounds."""
        # Normalizes the result (needed for UUIDs) after the min/max computation
        select = self.make_select().select(
            ApplyFuncAndNormalizeAsString(self._get_column_transforms(k), f) for k in self.key_columns for f in (min_, max_)
        )
        result = tuple(self.database.query(select, tuple))

        if any(i is None for i in result):
            # We return EmptyTable instead of raising it, so that we can consume
            # the key_ranges as an iterator.
            # _parse_key_range_result() will raise the error we return.
            return EmptyTable(f"Table {self.table_path} appears to be empty.", self)

        # Min/max keys are interleaved
        min_key, max_key = result[::2], result[1::2]
        assert len(min_key) == len(max_key)

        return min_key, max_key

    @property
    def is_bounded(self):
        return self.min_key is not None and self.max_key is not None

    def approximate_size(self):
        if not self.is_bounded:
            raise RuntimeError("Cannot approximate the size of an unbounded segment. Must have min_key and max_key.")
        diff = self.max_key - self.min_key
        assert all(d > 0 for d in diff)
        return int_product(diff)

    @property
    def key_types(self):
        return [self._schema[i] for i in self.key_columns]


@dataclass
class EmptyTableSegment:
    _table_segment: TableSegment

    def approximate_size(self):
        return 0

    @property
    def is_bounded(self):
        return True

    def query_key_range(self) -> Tuple[tuple, tuple]:
        return EmptyTable()

    def count(self) -> int:
        return 0

    def count_and_checksum(self) -> Tuple[int, int]:
        return (0, None)

    def __getattr__(self, attr):
        assert attr in ("database", "key_columns", "key_types", "relevant_columns", "_schema", "transform_columns", "_get_column_transforms")
        return getattr(self._table_segment, attr)

    @property
    def min_key(self):
        return None

    @property
    def max_key(self):
        return None

    def with_schema(self, refine: bool = True, allow_empty_table: bool = False) -> "TableSegment":
        assert self._table_segment._schema
        return self

    def new_key_bounds(self, min_key: Vector, max_key: Vector) -> "TableSegment":
        return self

    def segment_by_checkpoints(self, checkpoints: List[List[DbKey]]) -> List["TableSegment"]:
        "Split the current TableSegment to a bunch of smaller ones, separated by the given checkpoints"
        mesh = create_mesh_from_points(*checkpoints)
        return [self for s, e in mesh]

    def make_select(self):
        # XXX shouldn't be called
        return self._table_segment.make_select()

    def get_values(self) -> list:
        return []
