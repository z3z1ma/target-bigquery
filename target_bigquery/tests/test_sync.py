"""Tests standard target features using the built-in SDK tests library."""
import datetime
import io
import os
import time

import pytest
from google.api_core.exceptions import NotFound
from singer_sdk.testing import target_sync_test

from target_bigquery.core import BigQueryCredentials, bigquery_client_factory
from target_bigquery.target import TargetBigQuery

BASIC_SINGER_STREAM = """
{"type": "SCHEMA", "stream": "{stream_name}", "schema": {"properties": {"id": {"type": ["integer", "null"]}, "rep_key": {"type": ["integer", "null"]}, "Col_2_string": {"type": ["string", "null"]}, "Col_3_datetime": {"format": "date-time", "type": ["string", "null"]}, "Col_4_int": {"type": ["integer", "null"]}, "Col_5_float": {"type": ["number", "null"]}, "Col_6_string": {"type": ["string", "null"]}, "Col_7_datetime": {"format": "date-time", "type": ["string", "null"]}, "Col_8_int": {"type": ["integer", "null"]}, "Col_9_float": {"type": ["number", "null"]}, "Col_10_string": {"type": ["string", "null"]}, "Col_11_datetime": {"format": "date-time", "type": ["string", "null"]}, "Col_12_int": {"type": ["integer", "null"]}, "Col_13_float": {"type": ["number", "null"]}, "Col_14_string": {"type": ["string", "null"]}, "Col_15_datetime": {"format": "date-time", "type": ["string", "null"]}, "Col_16_int": {"type": ["integer", "null"]}, "Col_17_float": {"type": ["number", "null"]}, "Col_18_string": {"type": ["string", "null"]}, "Col_19_datetime": {"format": "date-time", "type": ["string", "null"]}, "Col_20_int": {"type": ["integer", "null"]}, "Col_21_float": {"type": ["number", "null"]}, "Col_22_string": {"type": ["string", "null"]}, "Col_23_datetime": {"format": "date-time", "type": ["string", "null"]}, "Col_24_int": {"type": ["integer", "null"]}, "Col_25_float": {"type": ["number", "null"]}, "Col_26_string": {"type": ["string", "null"]}, "Col_27_datetime": {"format": "date-time", "type": ["string", "null"]}, "Col_28_int": {"type": ["integer", "null"]}, "Col_29_float": {"type": ["number", "null"]}}, "type": "object"}, "key_properties": ["id"], "bookmark_properties": ["rep_key"]}
{"type": "RECORD", "stream": "{stream_name}", "record": {"id": 0, "rep_key": 0, "Col_2_string": "55fcfce1-f7ce-4f90-8a54-e92de8dae40d", "Col_3_datetime": "2022-11-11T13:40:56.806208+00:00", "Col_4_int": 1475701382, "Col_5_float": 0.7920345506520963, "Col_6_string": "744dddf7-d07b-4b09-baf1-8b1e4914632b", "Col_7_datetime": "2022-11-11T13:40:56.806225+00:00", "Col_8_int": 60612870, "Col_9_float": 0.35286203712175723, "Col_10_string": "9a164a0b-c6c7-46c5-8224-1ea0f7de485d", "Col_11_datetime": "2022-11-11T13:40:56.806233+00:00", "Col_12_int": 1024423793, "Col_13_float": 0.71921051655093, "Col_14_string": "c3630f45-660d-42c1-baf9-c58434d68786", "Col_15_datetime": "2022-11-11T13:40:56.806238+00:00", "Col_16_int": 556025207, "Col_17_float": 0.3965419777404805, "Col_18_string": "84ebc035-4b0c-40be-a9a3-61cbf7e0f01e", "Col_19_datetime": "2022-11-11T13:40:56.806243+00:00", "Col_20_int": 661621821, "Col_21_float": 0.37192361356880477, "Col_22_string": "41613825-9170-4a55-bfd6-c64222292573", "Col_23_datetime": "2022-11-11T13:40:56.806248+00:00", "Col_24_int": 1716807152, "Col_25_float": 0.9639895917683756, "Col_26_string": "87025878-196b-4990-9a65-ec06799fe34e", "Col_27_datetime": "2022-11-11T13:40:56.806253+00:00", "Col_28_int": 542678613, "Col_29_float": 0.4722333859761568}, "time_extracted": "2022-11-11T20:40:56.806676+00:00"}
{"type": "STATE", "value": {"bookmarks": {"{stream_name}": {"starting_replication_value": null, "replication_key": "rep_key", "replication_key_value": 0}}}}
{"type": "RECORD", "stream": "{stream_name}", "record": {"id": 1, "rep_key": 1, "Col_2_string": "757a799e-2f24-4b19-a3e0-2a8dc4789bd2", "Col_3_datetime": "2022-11-11T13:40:56.806844+00:00", "Col_4_int": 1735919329, "Col_5_float": 0.379524732206737, "Col_6_string": "6e9e168f-7876-46fc-a4bd-d407304b1140", "Col_7_datetime": "2022-11-11T13:40:56.806851+00:00", "Col_8_int": 115992044, "Col_9_float": 0.2211666724632626, "Col_10_string": "c5cb63ba-b0c1-4593-9610-934b9d231162", "Col_11_datetime": "2022-11-11T13:40:56.806857+00:00", "Col_12_int": 1441770413, "Col_13_float": 0.7992267452184196, "Col_14_string": "f905954a-7f61-45ec-acb0-3f1ef494961c", "Col_15_datetime": "2022-11-11T13:40:56.806862+00:00", "Col_16_int": 413289300, "Col_17_float": 0.2819810417062052, "Col_18_string": "e004eb72-7966-4325-827c-0aff81293a22", "Col_19_datetime": "2022-11-11T13:40:56.806869+00:00", "Col_20_int": 1429278155, "Col_21_float": 0.6876903186985626, "Col_22_string": "edb6102d-8577-44ae-bfc7-4de727194f26", "Col_23_datetime": "2022-11-11T13:40:56.806874+00:00", "Col_24_int": 1898676962, "Col_25_float": 0.029470842491415294, "Col_26_string": "c78aba99-9f9e-470a-bf62-89899c37ffab", "Col_27_datetime": "2022-11-11T13:40:56.806879+00:00", "Col_28_int": 669926127, "Col_29_float": 0.9353886259013682}, "time_extracted": "2022-11-11T20:40:56.807284+00:00"}
{"type": "RECORD", "stream": "{stream_name}", "record": {"id": 2, "rep_key": 2, "Col_2_string": "45d8f187-3776-46bc-bbba-195f70455b0d", "Col_3_datetime": "2022-11-11T13:40:56.807414+00:00", "Col_4_int": 1496147986, "Col_5_float": 0.6505589093769572, "Col_6_string": "e17c9332-531c-44fd-89d7-d71bb3071a4c", "Col_7_datetime": "2022-11-11T13:40:56.807419+00:00", "Col_8_int": 1483639242, "Col_9_float": 0.14177466670396877, "Col_10_string": "8ea89b6d-030b-4884-b2f3-aa7cb3f84a32", "Col_11_datetime": "2022-11-11T13:40:56.807425+00:00", "Col_12_int": 281713617, "Col_13_float": 0.9128294678949099, "Col_14_string": "b8c0287a-cb3b-471c-a73f-ddbbd46def96", "Col_15_datetime": "2022-11-11T13:40:56.807430+00:00", "Col_16_int": 1676536646, "Col_17_float": 0.20444489249202114, "Col_18_string": "3078fa1b-0d35-4393-a1bc-e005dde62494", "Col_19_datetime": "2022-11-11T13:40:56.807435+00:00", "Col_20_int": 1596646895, "Col_21_float": 0.13991626696903536, "Col_22_string": "5d033520-2214-4ab4-a586-09d9939fa8d0", "Col_23_datetime": "2022-11-11T13:40:56.807440+00:00", "Col_24_int": 339609379, "Col_25_float": 0.7179419896107613, "Col_26_string": "8c39c287-f4d7-4781-a7ed-6f87124866b6", "Col_27_datetime": "2022-11-11T13:40:56.807445+00:00", "Col_28_int": 1815034575, "Col_29_float": 0.9522143006195518}, "time_extracted": "2022-11-11T20:40:56.807796+00:00"}
{"type": "RECORD", "stream": "{stream_name}", "record": {"id": 3, "rep_key": 3, "Col_2_string": "19a9ceb8-9bda-46a0-9a19-f9117be34ddf", "Col_3_datetime": "2022-11-11T13:40:56.807910+00:00", "Col_4_int": 1957217254, "Col_5_float": 0.43289496913069425, "Col_6_string": "53ed2273-48d6-4182-9717-6d0af51e6ad9", "Col_7_datetime": "2022-11-11T13:40:56.807916+00:00", "Col_8_int": 1672454615, "Col_9_float": 0.45849249733339414, "Col_10_string": "e60b231d-5de8-4cdf-afa7-d591a60f0207", "Col_11_datetime": "2022-11-11T13:40:56.807921+00:00", "Col_12_int": 620242020, "Col_13_float": 0.12772702726587115, "Col_14_string": "4a00041b-e71b-4832-8188-9b35b4025d57", "Col_15_datetime": "2022-11-11T13:40:56.807926+00:00", "Col_16_int": 869791103, "Col_17_float": 0.7034707479613156, "Col_18_string": "9be8dbd7-71e9-4af6-8d6b-55badc74042e", "Col_19_datetime": "2022-11-11T13:40:56.807931+00:00", "Col_20_int": 1158691101, "Col_21_float": 0.961251074216243, "Col_22_string": "3488df2c-761b-4910-9a4f-8541517a577c", "Col_23_datetime": "2022-11-11T13:40:56.807936+00:00", "Col_24_int": 1803445752, "Col_25_float": 0.9712573557605182, "Col_26_string": "8b35b130-cc4b-4b13-8c4e-8c68b87b0b46", "Col_27_datetime": "2022-11-11T13:40:56.807941+00:00", "Col_28_int": 152329871, "Col_29_float": 0.07133900568240592}, "time_extracted": "2022-11-11T20:40:56.808289+00:00"}
{"type": "RECORD", "stream": "{stream_name}", "record": {"id": 4, "rep_key": 4, "Col_2_string": "f1fa6590-8b7a-4cfa-ad85-2839d5d358d6", "Col_3_datetime": "2022-11-11T13:40:56.808401+00:00", "Col_4_int": 1840786740, "Col_5_float": 0.8999649085844895, "Col_6_string": "d222b83a-a660-470e-9fa9-838e6240e395", "Col_7_datetime": "2022-11-11T13:40:56.808407+00:00", "Col_8_int": 1300812810, "Col_9_float": 0.4513495007382251, "Col_10_string": "bb719c2d-842b-4d4d-ad09-a8a1e8361218", "Col_11_datetime": "2022-11-11T13:40:56.808412+00:00", "Col_12_int": 451674926, "Col_13_float": 0.7040279698622643, "Col_14_string": "58dd57f6-2511-410e-9a57-3d72da46e569", "Col_15_datetime": "2022-11-11T13:40:56.808417+00:00", "Col_16_int": 1824274251, "Col_17_float": 0.5618479908554109, "Col_18_string": "627e9b1f-5ac2-43d0-8c71-173fe7fcd33f", "Col_19_datetime": "2022-11-11T13:40:56.808422+00:00", "Col_20_int": 1379996338, "Col_21_float": 0.10080587804819452, "Col_22_string": "a21ee366-8cb7-4fb3-8ab4-ae897c72f303", "Col_23_datetime": "2022-11-11T13:40:56.808427+00:00", "Col_24_int": 329557210, "Col_25_float": 0.9385358324655749, "Col_26_string": "b22e4ba1-eb9f-4d2a-8390-6365c3791eb6", "Col_27_datetime": "2022-11-11T13:40:56.808432+00:00", "Col_28_int": 1021771707, "Col_29_float": 0.22978526041580527}, "time_extracted": "2022-11-11T20:40:56.808782+00:00"}
{"type": "SCHEMA", "stream": "{stream_name_2}", "schema": {"properties": {"id": {"type": ["integer", "null"]}, "rep_key": {"type": ["integer", "null"]}}, "type": "object"}, "key_properties": ["id"], "bookmark_properties": ["rep_key"]}
{"type": "RECORD", "stream": "{stream_name_2}", "record": {"id": 0, "rep_key": 0}, "time_extracted": "2022-11-11T20:40:56.806676+00:00"}
{"type": "STATE", "value": {"bookmarks": {"{stream_name_2}": {"starting_replication_value": null, "replication_key": "rep_key", "replication_key_value": 0}}}}
{"type": "RECORD", "stream": "{stream_name_2}", "record": {"id": 1, "rep_key": 1}, "time_extracted": "2022-11-11T20:40:56.807284+00:00"}
{"type": "RECORD", "stream": "{stream_name_2}", "record": {"id": 2, "rep_key": 2}, "time_extracted": "2022-11-11T20:40:56.807796+00:00"}
{"type": "RECORD", "stream": "{stream_name_2}", "record": {"id": 3, "rep_key": 3}, "time_extracted": "2022-11-11T20:40:56.808289+00:00"}
{"type": "RECORD", "stream": "{stream_name_2}", "record": {"id": 4, "rep_key": 4}, "time_extracted": "2022-11-11T20:40:56.808782+00:00"}
""".strip()


@pytest.mark.parametrize(
    "method",
    ["batch_job", "streaming_insert", "storage_write_api", "gcs_stage"],
    ids=["batch_job", "streaming_insert", "storage_write_api", "gcs_stage"],
)
@pytest.mark.parametrize("batch_mode", [False, True], ids=["no_batch_mode", "batch_mode"])
def test_basic_sync(method, batch_mode):
    OPTS = {
        "method": method,
        "denormalized": False,
        "generate_view": False,
        "batch_size": 2,  # force multiple batches
        "options": {"storage_write_batch_mode": batch_mode},
    }

    table_name = method
    if batch_mode:
        table_name += "_batch"
    if OPTS["denormalized"]:
        table_name += "_denorm"
    if "PYTHON_VERSION" in os.environ:
        table_name += f"_py{os.environ['PYTHON_VERSION'].replace('.', '')}"
    table_name_2 = table_name + "_2"

    singer_input = io.StringIO()
    singer_input.write(
        BASIC_SINGER_STREAM.replace("{stream_name}", table_name).replace(
            "{stream_name_2}", table_name_2
        )
    )

    singer_input.seek(0)

    target = TargetBigQuery(
        config={
            "credentials_json": os.environ["BQ_CREDS"],
            "project": os.environ["BQ_PROJECT"],
            "dataset": os.environ["BQ_DATASET"],
            "bucket": os.environ["GCS_BUCKET"],
            **OPTS,
        },
    )

    client = bigquery_client_factory(BigQueryCredentials(json=target.config["credentials_json"]))
    try:
        client.query(f"TRUNCATE TABLE {target.config['dataset']}.{table_name}").result()
    except NotFound:
        pass
    try:
        client.query(f"TRUNCATE TABLE {target.config['dataset']}.{table_name_2}").result()
    except NotFound:
        pass

    stdout, stderr = target_sync_test(target, singer_input)
    del stdout, stderr
    time.sleep(5)  # wait for the eventual consistency seen in LoadJob sinks

    records = [
        dict(record)
        for record in client.query(
            f"SELECT data FROM {target.config['dataset']}.{table_name} ORDER BY INT64(data.id)"
        ).result()
    ]
    records_2 = [
        dict(record)
        for record in client.query(
            f"SELECT data FROM {target.config['dataset']}.{table_name_2} ORDER BY INT64(data.id)"
        ).result()
    ]

    assert len(records) == 5
    assert len(records_2) == 5
    assert records == [
        {
            "data": '{"Col_10_string":"9a164a0b-c6c7-46c5-8224-1ea0f7de485d","Col_11_datetime":"2022-11-11T13:40:56.806233+00:00","Col_12_int":1024423793,"Col_13_float":0.71921051655093,"Col_14_string":"c3630f45-660d-42c1-baf9-c58434d68786","Col_15_datetime":"2022-11-11T13:40:56.806238+00:00","Col_16_int":556025207,"Col_17_float":0.3965419777404805,"Col_18_string":"84ebc035-4b0c-40be-a9a3-61cbf7e0f01e","Col_19_datetime":"2022-11-11T13:40:56.806243+00:00","Col_20_int":661621821,"Col_21_float":0.37192361356880477,"Col_22_string":"41613825-9170-4a55-bfd6-c64222292573","Col_23_datetime":"2022-11-11T13:40:56.806248+00:00","Col_24_int":1716807152,"Col_25_float":0.9639895917683756,"Col_26_string":"87025878-196b-4990-9a65-ec06799fe34e","Col_27_datetime":"2022-11-11T13:40:56.806253+00:00","Col_28_int":542678613,"Col_29_float":0.4722333859761568,"Col_2_string":"55fcfce1-f7ce-4f90-8a54-e92de8dae40d","Col_3_datetime":"2022-11-11T13:40:56.806208+00:00","Col_4_int":1475701382,"Col_5_float":0.7920345506520963,"Col_6_string":"744dddf7-d07b-4b09-baf1-8b1e4914632b","Col_7_datetime":"2022-11-11T13:40:56.806225+00:00","Col_8_int":60612870,"Col_9_float":0.35286203712175723,"id":0,"rep_key":0}'
        },
        {
            "data": '{"Col_10_string":"c5cb63ba-b0c1-4593-9610-934b9d231162","Col_11_datetime":"2022-11-11T13:40:56.806857+00:00","Col_12_int":1441770413,"Col_13_float":0.7992267452184196,"Col_14_string":"f905954a-7f61-45ec-acb0-3f1ef494961c","Col_15_datetime":"2022-11-11T13:40:56.806862+00:00","Col_16_int":413289300,"Col_17_float":0.2819810417062052,"Col_18_string":"e004eb72-7966-4325-827c-0aff81293a22","Col_19_datetime":"2022-11-11T13:40:56.806869+00:00","Col_20_int":1429278155,"Col_21_float":0.6876903186985626,"Col_22_string":"edb6102d-8577-44ae-bfc7-4de727194f26","Col_23_datetime":"2022-11-11T13:40:56.806874+00:00","Col_24_int":1898676962,"Col_25_float":0.029470842491415294,"Col_26_string":"c78aba99-9f9e-470a-bf62-89899c37ffab","Col_27_datetime":"2022-11-11T13:40:56.806879+00:00","Col_28_int":669926127,"Col_29_float":0.9353886259013682,"Col_2_string":"757a799e-2f24-4b19-a3e0-2a8dc4789bd2","Col_3_datetime":"2022-11-11T13:40:56.806844+00:00","Col_4_int":1735919329,"Col_5_float":0.379524732206737,"Col_6_string":"6e9e168f-7876-46fc-a4bd-d407304b1140","Col_7_datetime":"2022-11-11T13:40:56.806851+00:00","Col_8_int":115992044,"Col_9_float":0.2211666724632626,"id":1,"rep_key":1}'
        },
        {
            "data": '{"Col_10_string":"8ea89b6d-030b-4884-b2f3-aa7cb3f84a32","Col_11_datetime":"2022-11-11T13:40:56.807425+00:00","Col_12_int":281713617,"Col_13_float":0.9128294678949099,"Col_14_string":"b8c0287a-cb3b-471c-a73f-ddbbd46def96","Col_15_datetime":"2022-11-11T13:40:56.807430+00:00","Col_16_int":1676536646,"Col_17_float":0.20444489249202114,"Col_18_string":"3078fa1b-0d35-4393-a1bc-e005dde62494","Col_19_datetime":"2022-11-11T13:40:56.807435+00:00","Col_20_int":1596646895,"Col_21_float":0.13991626696903536,"Col_22_string":"5d033520-2214-4ab4-a586-09d9939fa8d0","Col_23_datetime":"2022-11-11T13:40:56.807440+00:00","Col_24_int":339609379,"Col_25_float":0.7179419896107613,"Col_26_string":"8c39c287-f4d7-4781-a7ed-6f87124866b6","Col_27_datetime":"2022-11-11T13:40:56.807445+00:00","Col_28_int":1815034575,"Col_29_float":0.9522143006195518,"Col_2_string":"45d8f187-3776-46bc-bbba-195f70455b0d","Col_3_datetime":"2022-11-11T13:40:56.807414+00:00","Col_4_int":1496147986,"Col_5_float":0.6505589093769572,"Col_6_string":"e17c9332-531c-44fd-89d7-d71bb3071a4c","Col_7_datetime":"2022-11-11T13:40:56.807419+00:00","Col_8_int":1483639242,"Col_9_float":0.14177466670396877,"id":2,"rep_key":2}'
        },
        {
            "data": '{"Col_10_string":"e60b231d-5de8-4cdf-afa7-d591a60f0207","Col_11_datetime":"2022-11-11T13:40:56.807921+00:00","Col_12_int":620242020,"Col_13_float":0.12772702726587115,"Col_14_string":"4a00041b-e71b-4832-8188-9b35b4025d57","Col_15_datetime":"2022-11-11T13:40:56.807926+00:00","Col_16_int":869791103,"Col_17_float":0.7034707479613156,"Col_18_string":"9be8dbd7-71e9-4af6-8d6b-55badc74042e","Col_19_datetime":"2022-11-11T13:40:56.807931+00:00","Col_20_int":1158691101,"Col_21_float":0.961251074216243,"Col_22_string":"3488df2c-761b-4910-9a4f-8541517a577c","Col_23_datetime":"2022-11-11T13:40:56.807936+00:00","Col_24_int":1803445752,"Col_25_float":0.9712573557605182,"Col_26_string":"8b35b130-cc4b-4b13-8c4e-8c68b87b0b46","Col_27_datetime":"2022-11-11T13:40:56.807941+00:00","Col_28_int":152329871,"Col_29_float":0.07133900568240592,"Col_2_string":"19a9ceb8-9bda-46a0-9a19-f9117be34ddf","Col_3_datetime":"2022-11-11T13:40:56.807910+00:00","Col_4_int":1957217254,"Col_5_float":0.43289496913069425,"Col_6_string":"53ed2273-48d6-4182-9717-6d0af51e6ad9","Col_7_datetime":"2022-11-11T13:40:56.807916+00:00","Col_8_int":1672454615,"Col_9_float":0.45849249733339414,"id":3,"rep_key":3}'
        },
        {
            "data": '{"Col_10_string":"bb719c2d-842b-4d4d-ad09-a8a1e8361218","Col_11_datetime":"2022-11-11T13:40:56.808412+00:00","Col_12_int":451674926,"Col_13_float":0.7040279698622643,"Col_14_string":"58dd57f6-2511-410e-9a57-3d72da46e569","Col_15_datetime":"2022-11-11T13:40:56.808417+00:00","Col_16_int":1824274251,"Col_17_float":0.5618479908554109,"Col_18_string":"627e9b1f-5ac2-43d0-8c71-173fe7fcd33f","Col_19_datetime":"2022-11-11T13:40:56.808422+00:00","Col_20_int":1379996338,"Col_21_float":0.10080587804819452,"Col_22_string":"a21ee366-8cb7-4fb3-8ab4-ae897c72f303","Col_23_datetime":"2022-11-11T13:40:56.808427+00:00","Col_24_int":329557210,"Col_25_float":0.9385358324655749,"Col_26_string":"b22e4ba1-eb9f-4d2a-8390-6365c3791eb6","Col_27_datetime":"2022-11-11T13:40:56.808432+00:00","Col_28_int":1021771707,"Col_29_float":0.22978526041580527,"Col_2_string":"f1fa6590-8b7a-4cfa-ad85-2839d5d358d6","Col_3_datetime":"2022-11-11T13:40:56.808401+00:00","Col_4_int":1840786740,"Col_5_float":0.8999649085844895,"Col_6_string":"d222b83a-a660-470e-9fa9-838e6240e395","Col_7_datetime":"2022-11-11T13:40:56.808407+00:00","Col_8_int":1300812810,"Col_9_float":0.4513495007382251,"id":4,"rep_key":4}'
        },
    ]


@pytest.mark.parametrize(
    "method",
    ["batch_job", "streaming_insert", "gcs_stage", "storage_write_api"],
    ids=["batch_job", "streaming_insert", "gcs_stage", "storage_write_api"],
)
def test_basic_denorm_sync(method):
    OPTS = {
        "method": method,
        "denormalized": True,
        "generate_view": False,
        "batch_size": 2,  # force multiple batches
    }

    table_name = OPTS["method"]
    if OPTS["denormalized"]:
        table_name += "_denorm"
    if "PYTHON_VERSION" in os.environ:
        table_name += f"_py{os.environ['PYTHON_VERSION'].replace('.', '')}"

    singer_input = io.StringIO()
    singer_input.write(BASIC_SINGER_STREAM.replace("{stream_name}", table_name))
    singer_input.seek(0)

    target = TargetBigQuery(
        config={
            "credentials_json": os.environ["BQ_CREDS"],
            "project": os.environ["BQ_PROJECT"],
            "dataset": os.environ["BQ_DATASET"],
            "bucket": os.environ["GCS_BUCKET"],
            **OPTS,
        },
    )

    client = bigquery_client_factory(BigQueryCredentials(json=target.config["credentials_json"]))
    try:
        client.query(f"TRUNCATE TABLE {target.config['dataset']}.{table_name}").result()
    except NotFound:
        pass

    stdout, stderr = target_sync_test(target, singer_input)
    del stdout, stderr
    time.sleep(10)  # wait for the eventual consistency seen in LoadJobs sinks

    records = [
        dict(record)
        for record in client.query(
            "SELECT * except(_sdc_extracted_at, _sdc_received_at, _sdc_batched_at,"
            " _sdc_deleted_at, _sdc_sequence, _sdc_table_version) FROM"
            f" {target.config['dataset']}.{table_name} ORDER BY id"
        ).result()
    ]

    assert len(records) == 5
    assert records == [
        {
            "id": 0,
            "rep_key": 0,
            "Col_2_string": "55fcfce1-f7ce-4f90-8a54-e92de8dae40d",
            "Col_3_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806208, tzinfo=datetime.timezone.utc
            ),
            "Col_4_int": 1475701382,
            "Col_5_float": 0.7920345506520963,
            "Col_6_string": "744dddf7-d07b-4b09-baf1-8b1e4914632b",
            "Col_7_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806225, tzinfo=datetime.timezone.utc
            ),
            "Col_8_int": 60612870,
            "Col_9_float": 0.35286203712175723,
            "Col_10_string": "9a164a0b-c6c7-46c5-8224-1ea0f7de485d",
            "Col_11_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806233, tzinfo=datetime.timezone.utc
            ),
            "Col_12_int": 1024423793,
            "Col_13_float": 0.71921051655093,
            "Col_14_string": "c3630f45-660d-42c1-baf9-c58434d68786",
            "Col_15_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806238, tzinfo=datetime.timezone.utc
            ),
            "Col_16_int": 556025207,
            "Col_17_float": 0.3965419777404805,
            "Col_18_string": "84ebc035-4b0c-40be-a9a3-61cbf7e0f01e",
            "Col_19_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806243, tzinfo=datetime.timezone.utc
            ),
            "Col_20_int": 661621821,
            "Col_21_float": 0.37192361356880477,
            "Col_22_string": "41613825-9170-4a55-bfd6-c64222292573",
            "Col_23_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806248, tzinfo=datetime.timezone.utc
            ),
            "Col_24_int": 1716807152,
            "Col_25_float": 0.9639895917683756,
            "Col_26_string": "87025878-196b-4990-9a65-ec06799fe34e",
            "Col_27_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806253, tzinfo=datetime.timezone.utc
            ),
            "Col_28_int": 542678613,
            "Col_29_float": 0.4722333859761568,
        },
        {
            "id": 1,
            "rep_key": 1,
            "Col_2_string": "757a799e-2f24-4b19-a3e0-2a8dc4789bd2",
            "Col_3_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806844, tzinfo=datetime.timezone.utc
            ),
            "Col_4_int": 1735919329,
            "Col_5_float": 0.379524732206737,
            "Col_6_string": "6e9e168f-7876-46fc-a4bd-d407304b1140",
            "Col_7_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806851, tzinfo=datetime.timezone.utc
            ),
            "Col_8_int": 115992044,
            "Col_9_float": 0.2211666724632626,
            "Col_10_string": "c5cb63ba-b0c1-4593-9610-934b9d231162",
            "Col_11_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806857, tzinfo=datetime.timezone.utc
            ),
            "Col_12_int": 1441770413,
            "Col_13_float": 0.7992267452184196,
            "Col_14_string": "f905954a-7f61-45ec-acb0-3f1ef494961c",
            "Col_15_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806862, tzinfo=datetime.timezone.utc
            ),
            "Col_16_int": 413289300,
            "Col_17_float": 0.2819810417062052,
            "Col_18_string": "e004eb72-7966-4325-827c-0aff81293a22",
            "Col_19_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806869, tzinfo=datetime.timezone.utc
            ),
            "Col_20_int": 1429278155,
            "Col_21_float": 0.6876903186985626,
            "Col_22_string": "edb6102d-8577-44ae-bfc7-4de727194f26",
            "Col_23_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806874, tzinfo=datetime.timezone.utc
            ),
            "Col_24_int": 1898676962,
            "Col_25_float": 0.029470842491415294,
            "Col_26_string": "c78aba99-9f9e-470a-bf62-89899c37ffab",
            "Col_27_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 806879, tzinfo=datetime.timezone.utc
            ),
            "Col_28_int": 669926127,
            "Col_29_float": 0.9353886259013682,
        },
        {
            "id": 2,
            "rep_key": 2,
            "Col_2_string": "45d8f187-3776-46bc-bbba-195f70455b0d",
            "Col_3_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807414, tzinfo=datetime.timezone.utc
            ),
            "Col_4_int": 1496147986,
            "Col_5_float": 0.6505589093769572,
            "Col_6_string": "e17c9332-531c-44fd-89d7-d71bb3071a4c",
            "Col_7_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807419, tzinfo=datetime.timezone.utc
            ),
            "Col_8_int": 1483639242,
            "Col_9_float": 0.14177466670396877,
            "Col_10_string": "8ea89b6d-030b-4884-b2f3-aa7cb3f84a32",
            "Col_11_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807425, tzinfo=datetime.timezone.utc
            ),
            "Col_12_int": 281713617,
            "Col_13_float": 0.9128294678949099,
            "Col_14_string": "b8c0287a-cb3b-471c-a73f-ddbbd46def96",
            "Col_15_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807430, tzinfo=datetime.timezone.utc
            ),
            "Col_16_int": 1676536646,
            "Col_17_float": 0.20444489249202114,
            "Col_18_string": "3078fa1b-0d35-4393-a1bc-e005dde62494",
            "Col_19_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807435, tzinfo=datetime.timezone.utc
            ),
            "Col_20_int": 1596646895,
            "Col_21_float": 0.13991626696903536,
            "Col_22_string": "5d033520-2214-4ab4-a586-09d9939fa8d0",
            "Col_23_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807440, tzinfo=datetime.timezone.utc
            ),
            "Col_24_int": 339609379,
            "Col_25_float": 0.7179419896107613,
            "Col_26_string": "8c39c287-f4d7-4781-a7ed-6f87124866b6",
            "Col_27_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807445, tzinfo=datetime.timezone.utc
            ),
            "Col_28_int": 1815034575,
            "Col_29_float": 0.9522143006195518,
        },
        {
            "id": 3,
            "rep_key": 3,
            "Col_2_string": "19a9ceb8-9bda-46a0-9a19-f9117be34ddf",
            "Col_3_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807910, tzinfo=datetime.timezone.utc
            ),
            "Col_4_int": 1957217254,
            "Col_5_float": 0.43289496913069425,
            "Col_6_string": "53ed2273-48d6-4182-9717-6d0af51e6ad9",
            "Col_7_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807916, tzinfo=datetime.timezone.utc
            ),
            "Col_8_int": 1672454615,
            "Col_9_float": 0.45849249733339414,
            "Col_10_string": "e60b231d-5de8-4cdf-afa7-d591a60f0207",
            "Col_11_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807921, tzinfo=datetime.timezone.utc
            ),
            "Col_12_int": 620242020,
            "Col_13_float": 0.12772702726587115,
            "Col_14_string": "4a00041b-e71b-4832-8188-9b35b4025d57",
            "Col_15_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807926, tzinfo=datetime.timezone.utc
            ),
            "Col_16_int": 869791103,
            "Col_17_float": 0.7034707479613156,
            "Col_18_string": "9be8dbd7-71e9-4af6-8d6b-55badc74042e",
            "Col_19_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807931, tzinfo=datetime.timezone.utc
            ),
            "Col_20_int": 1158691101,
            "Col_21_float": 0.961251074216243,
            "Col_22_string": "3488df2c-761b-4910-9a4f-8541517a577c",
            "Col_23_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807936, tzinfo=datetime.timezone.utc
            ),
            "Col_24_int": 1803445752,
            "Col_25_float": 0.9712573557605182,
            "Col_26_string": "8b35b130-cc4b-4b13-8c4e-8c68b87b0b46",
            "Col_27_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 807941, tzinfo=datetime.timezone.utc
            ),
            "Col_28_int": 152329871,
            "Col_29_float": 0.07133900568240592,
        },
        {
            "id": 4,
            "rep_key": 4,
            "Col_2_string": "f1fa6590-8b7a-4cfa-ad85-2839d5d358d6",
            "Col_3_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 808401, tzinfo=datetime.timezone.utc
            ),
            "Col_4_int": 1840786740,
            "Col_5_float": 0.8999649085844895,
            "Col_6_string": "d222b83a-a660-470e-9fa9-838e6240e395",
            "Col_7_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 808407, tzinfo=datetime.timezone.utc
            ),
            "Col_8_int": 1300812810,
            "Col_9_float": 0.4513495007382251,
            "Col_10_string": "bb719c2d-842b-4d4d-ad09-a8a1e8361218",
            "Col_11_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 808412, tzinfo=datetime.timezone.utc
            ),
            "Col_12_int": 451674926,
            "Col_13_float": 0.7040279698622643,
            "Col_14_string": "58dd57f6-2511-410e-9a57-3d72da46e569",
            "Col_15_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 808417, tzinfo=datetime.timezone.utc
            ),
            "Col_16_int": 1824274251,
            "Col_17_float": 0.5618479908554109,
            "Col_18_string": "627e9b1f-5ac2-43d0-8c71-173fe7fcd33f",
            "Col_19_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 808422, tzinfo=datetime.timezone.utc
            ),
            "Col_20_int": 1379996338,
            "Col_21_float": 0.10080587804819452,
            "Col_22_string": "a21ee366-8cb7-4fb3-8ab4-ae897c72f303",
            "Col_23_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 808427, tzinfo=datetime.timezone.utc
            ),
            "Col_24_int": 329557210,
            "Col_25_float": 0.9385358324655749,
            "Col_26_string": "b22e4ba1-eb9f-4d2a-8390-6365c3791eb6",
            "Col_27_datetime": datetime.datetime(
                2022, 11, 11, 13, 40, 56, 808432, tzinfo=datetime.timezone.utc
            ),
            "Col_28_int": 1021771707,
            "Col_29_float": 0.22978526041580527,
        },
    ]
