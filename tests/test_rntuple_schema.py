from skhep_testdata import data_path  # type: ignore[import-not-found]

from rootfilespec.bootstrap import ROOT3a3aRNTuple
from rootfilespec.reader import open_path
from rootfilespec.rntuple.RNTuple import RNTuple
from rootfilespec.rntuple.schema import ColumnType


def test_column_value_range_is_double():
    """Issue #75: a column's value range is a pair of IEEE 754 doubles

    The file's quantized columns (4 to 10, fields quant1 to quant32) all have
    the range [-2.0, 3.0]. Read as int64, the same bytes are the bit patterns
    -4611686018427387904 and 4613937818241073152.
    """
    filename = "test_float_types_rntuple_v1-0-0-0.root"
    with open_path(data_path(filename)) as reader:
        anchor = reader.fetch(reader.keylist()["ntuple"])
        assert isinstance(anchor, ROOT3a3aRNTuple)
        rntuple = RNTuple.from_anchor(anchor, reader.fetch.buffer)

    columns = rntuple.schemaDescription.columnDescriptions
    with_range = [i for i, c in enumerate(columns) if c.fFlags & 0x02]
    assert with_range == list(range(4, 11))
    for i in with_range:
        column = columns[i]
        assert column.fColumnType == ColumnType.kReal32Quant
        assert column.fMinValue == -2.0
        assert column.fMaxValue == 3.0
        assert isinstance(column.fMinValue, float)
    for i, column in enumerate(columns):
        if i not in with_range:
            assert column.fMinValue is None
            assert column.fMaxValue is None
