"""Property tests for pure schema transformation helpers."""

from hypothesis import given
from hypothesis import strategies as st

from target_bigquery.core import bigquery_type, transform_column_name

column_names = st.text(
    alphabet=st.characters(
        blacklist_categories=["Cs"],
        blacklist_characters=("`", "\x00"),
    ),
    min_size=1,
    max_size=80,
)


@given(column_names)
def test_quoted_transform_preserves_wrapping(name: str):
    transformed = transform_column_name(name, quote=True)

    assert transformed.startswith("`")
    assert transformed.endswith("`")
    assert transformed.strip("`") == name


@given(column_names)
def test_period_replacement_removes_periods(name: str):
    transformed = transform_column_name(name, replace_period_with_underscore=True)

    assert "." not in transformed
    assert transformed == name.replace(".", "_")


@given(st.text(alphabet=st.characters(min_codepoint=48, max_codepoint=57), min_size=1))
def test_invalid_numeric_prefix_gets_single_leading_underscore(name: str):
    transformed = transform_column_name(name, add_underscore_when_invalid=True)

    assert transformed == f"_{name}"


@given(st.sampled_from(["date-time", "date", "time"]))
def test_bigquery_type_format_takes_precedence(property_format: str):
    expected = {"date-time": "timestamp", "date": "date", "time": "time"}[property_format]

    assert bigquery_type("string", property_format) == expected
    assert bigquery_type(["number", "integer", "boolean"], property_format) == expected


@given(st.lists(st.sampled_from(["integer", "number", "string", "boolean", "object"]), min_size=1))
def test_bigquery_type_always_returns_known_type(property_type: list[str]):
    assert bigquery_type(property_type) in {
        "boolean",
        "float",
        "integer",
        "record",
        "string",
    }
