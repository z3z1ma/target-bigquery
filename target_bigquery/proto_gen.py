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
from typing import Any, Dict, List, Optional, Type, cast

import proto
from google.cloud.bigquery import SchemaField
from google.protobuf import descriptor_pb2, message_factory

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


def generate_field_v2(
    base: SchemaField, i: int = 1, pool: Optional[Any] = None
) -> Dict[str, Any]:
    """Generate proto2 field properties from a SchemaField."""
    name: str = base.name
    typ: str = cast(str, base.field_type).upper()

    if base.mode == "REPEATED":
        label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
    else:
        label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

    if typ.upper() == "RECORD":
        proto_cls = proto_schema_factory_v2(base.fields, pool)
        props = dict(
            name=name,
            number=i,
            label=label,
            type=descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
            type_name=proto_cls.DESCRIPTOR.full_name,
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
    bigquery_schema: List[SchemaField], pool: Optional[Any] = None
) -> Type[proto.Message]:
    """Generate a proto2 Message from a BigQuery schema."""
    fhash = hashlib.sha1()
    for f in bigquery_schema:
        fhash.update(hash(f).to_bytes(8, "big", signed=True))
    fname = f"AnonymousProto_{fhash.hexdigest()}.proto"
    clsname = (
        f"net.proto2.python.public.target_bigquery.AnonymousProto_{fhash.hexdigest()}"
    )
    factory = message_factory.MessageFactory(pool=pool)
    try:
        proto_descriptor = factory.pool.FindMessageTypeByName(clsname)
        proto_cls = factory.GetPrototype(proto_descriptor)
    except KeyError:
        package, name = clsname.rsplit(".", 1)
        file_proto = descriptor_pb2.FileDescriptorProto()
        file_proto.name = os.path.join(package.replace(".", "/"), fname)
        file_proto.package = package
        desc_proto = file_proto.message_type.add()
        desc_proto.name = name
        for i, f in enumerate(bigquery_schema):
            field_proto = desc_proto.field.add()
            for k, v in generate_field_v2(f, i + 1, factory.pool).items():
                setattr(field_proto, k, v)
        factory.pool.Add(file_proto)
        proto_descriptor = factory.pool.FindMessageTypeByName(clsname)
        proto_cls = factory.GetPrototype(proto_descriptor)
    return proto_cls


def generate_field(base: SchemaField, i: int = 1) -> proto.Field:
    """Not intended for production use.
    Generate a proto.Field from a SchemaField."""
    kwargs = {}
    name: str = base.name
    typ: str = cast(str, base.field_type).upper()

    if base.mode == "REPEATED":
        cls = proto.RepeatedField
    else:
        cls = proto.Field

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


def proto_schema_factory(bigquery_schema: List[SchemaField]) -> Type[proto.Message]:
    """Not intended for production use.
    Generate a proto.Message from a BigQuery schema."""
    return type(
        f"Schema{abs(hash((f for f in bigquery_schema)))}",
        (proto.Message,),
        {
            name: f
            for f, name in (
                generate_field(field, i + 1) for i, field in enumerate(bigquery_schema)
            )
        },
    )
