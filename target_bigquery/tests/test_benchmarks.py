"""Benchmarks for pure transformation hot paths."""

from target_bigquery.core import SchemaTranslator


def test_schema_translation_benchmark(benchmark):
    schema = {
        "type": "object",
        "properties": {
            f"NestedColumn{i}": {
                "type": "object",
                "properties": {
                    "IntColumn": {"type": "integer"},
                    "StringColumn": {"type": "string"},
                    "ArrayColumn": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {"InnerColumn": {"type": "number"}},
                        },
                    },
                },
            }
            for i in range(20)
        },
    }

    def translate() -> None:
        translated = SchemaTranslator(schema, {"snake_case": True}).translated_schema_transformed
        assert len(translated) == 20

    benchmark(translate)
