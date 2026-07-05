# Copyright (c) 2023 Alex Butler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.
import hashlib
import os
import re
from collections.abc import Iterable
from typing import Any

import proto
from google.cloud.bigquery import SchemaField
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

MAP = {
    # Limited scope of types as these are the only primitive types we can infer
    # from the JSON schema which incidentally simplifies the implementation
    "FLOAT": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
    "INTEGER": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    "BOOLEAN": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    "STRING": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    # On ingestion, JSON is expected to be a string
    "JSON": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    # BigQuery coerces these to the correct format on ingestion
    "TIMESTAMP": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "DATE": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "TIME": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
}


def _proto_message_name(name: str) -> str:
    """Return a valid nested proto message name for a BigQuery field name."""
    safe = re.sub(r"[^0-9A-Za-z_]", "_", name)
    if not safe or safe[0].isdigit():
        safe = f"Field_{safe}"
    return f"Nested_{safe}"


def _populate_descriptor(
    desc_proto: descriptor_pb2.DescriptorProto,
    bigquery_schema: Iterable[SchemaField],
    *,
    package: str,
    path: list[str],
) -> None:
    """Populate a message descriptor with fields and nested message definitions."""
    for i, field in enumerate(bigquery_schema, start=1):
        field_proto = desc_proto.field.add()
        name = field.name
        typ = field.field_type.upper()
        field_proto.name = name
        field_proto.number = i
        field_proto.label = (
            descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
            if field.mode == "REPEATED"
            else descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL
        )
        field_proto.json_name = name
        if typ == "RECORD":
            nested_proto = desc_proto.nested_type.add()
            nested_proto.name = _proto_message_name(name)
            _populate_descriptor(
                nested_proto,
                field.fields,
                package=package,
                path=[*path, nested_proto.name],
            )
            field_proto.type = descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE
            field_proto.type_name = ".".join([package, *path, nested_proto.name])
        else:
            field_proto.type = MAP[typ]


def generate_field_v2(base: SchemaField, i: int = 1, pool: Any | None = None) -> dict[str, Any]:
    """Generate proto2 field properties from a SchemaField."""
    name: str = base.name
    typ = base.field_type.upper()

    if base.mode == "REPEATED":
        label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    else:
        label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

    if typ.upper() == "RECORD":
        proto_cls = proto_schema_factory_v2(list(base.fields), pool)
        props = dict(
            name=name,
            number=i,
            label=label,
            type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
            type_name=proto_cls.DESCRIPTOR.full_name,  # type: ignore
            json_name=name,
        )
    else:
        props = dict(
            name=name,
            number=i,
            label=label,
            type=MAP[typ],
            json_name=name,
        )

    return props


def proto_schema_factory_v2(
    bigquery_schema: list[SchemaField], pool: Any | None = None
) -> type[proto.Message]:
    """Generate a proto2 Message from a BigQuery schema."""
    fhash = hashlib.sha256()
    for f in bigquery_schema:
        fhash.update(hash(f).to_bytes(8, "big", signed=True))
    fname = f"AnonymousProto_{fhash.hexdigest()}.proto"
    clsname = f"net.proto2.python.public.target_bigquery.AnonymousProto_{fhash.hexdigest()}"

    # Use the pool directly if provided, otherwise use the default pool
    if pool is None:
        pool = descriptor_pool.Default()

    try:
        proto_descriptor = pool.FindMessageTypeByName(clsname)
        proto_cls = message_factory.GetMessageClass(proto_descriptor)
    except KeyError:
        package, name = clsname.rsplit(".", 1)
        file_proto = descriptor_pb2.FileDescriptorProto()
        file_proto.name = os.path.join(package.replace(".", "/"), fname)
        file_proto.package = package
        desc_proto = file_proto.message_type.add()
        desc_proto.name = name
        _populate_descriptor(desc_proto, bigquery_schema, package=package, path=[name])
        pool.Add(file_proto)
        proto_descriptor = pool.FindMessageTypeByName(clsname)
        proto_cls = message_factory.GetMessageClass(proto_descriptor)
    return proto_cls  # type: ignore


def generate_field(base: SchemaField, i: int = 1) -> tuple[proto.Field, str]:
    """Not intended for production use.
    Generate a proto.Field from a SchemaField."""
    kwargs: dict[str, Any] = {}
    name: str = base.name
    typ = base.field_type.upper()

    cls = proto.RepeatedField if base.mode == "REPEATED" else proto.Field

    if typ.upper() == "RECORD":
        f = cls(
            proto_type=proto.MESSAGE,
            message=proto_schema_factory(base.fields),
            number=i,
            **kwargs,
        )
    else:
        f = cls(proto_type=MAP[typ], number=i, **kwargs)

    return (f, name)


def proto_schema_factory(bigquery_schema: Iterable[SchemaField]) -> type[proto.Message]:
    """Not intended for production use.
    Generate a proto.Message from a BigQuery schema."""
    return type(
        f"Schema{abs(hash(f for f in bigquery_schema))}",
        (proto.Message,),
        {
            name: f
            for f, name in (generate_field(field, i + 1) for i, field in enumerate(bigquery_schema))
        },
    )
