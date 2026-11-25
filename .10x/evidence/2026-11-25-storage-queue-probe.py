import contextlib
import io
import json
import multiprocessing as mp

from google.cloud import bigquery
from google.cloud.bigquery_storage_v1 import types, writer

from target_bigquery import core, storage_write
from target_bigquery.constants import DEFAULT_SCHEMA
from target_bigquery.target import TargetBigQuery


class Metadata:
    def get_dataset(self, ref):
        return bigquery.Dataset(ref)

    def get_table(self, ref):
        return bigquery.Table(ref, schema=DEFAULT_SCHEMA)


class OfflineAppend:
    def __init__(self, *args):
        self._closed = False

    def send(self, request):
        future = writer.AppendRowsFuture(None)
        future.set_result(types.AppendRowsResponse())
        return future

    def close(self):
        self._closed = True


def install_api_fakes():
    storage_write.storage_client_factory = lambda _: None
    storage_write.writer.AppendRowsStream = OfflineAppend


def offline_worker():
    install_api_fakes()
    worker = mp.current_process()
    type(worker).run(worker)


def probe(process_pool):
    mp.set_start_method("spawn", force=True)
    core.bigquery_client_factory = lambda _: Metadata()
    install_api_fakes()
    target = TargetBigQuery(
        config={
            "project": "offline",
            "dataset": "offline",
            "method": "storage_write_api",
            "batch_size": 1,
            "options": {"process_pool": process_pool, "max_workers": 1},
        }
    )
    factory = target.worker_factory
    created = []

    def create():
        worker = factory()
        if process_pool:
            worker.run = offline_worker
        created.append(worker)
        return worker

    target.worker_factory = create
    messages = [
        {
            "type": "SCHEMA",
            "stream": "rows",
            "schema": {"type": "object", "properties": {"id": {"type": "integer"}}},
            "key_properties": [],
        },
        {"type": "RECORD", "stream": "rows", "record": {"id": 1}},
        {"type": "STATE", "value": {"position": 1}},
    ]
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            target.process_lines(map(json.dumps, messages))
            target.process_endofpipe()
        print(
            json.dumps(
                {"process_pool": process_pool, "outcome": "returned", "state": output.getvalue()}
            )
        )
    except Exception as error:
        import traceback

        print(
            json.dumps(
                {
                    "process_pool": process_pool,
                    "outcome": type(error).__name__,
                    "trace": traceback.format_exc(),
                    "state": output.getvalue(),
                }
            )
        )
    finally:
        for worker in created:
            if worker.is_alive():
                target.queue.put(None)
            worker.join(5)
            if worker.is_alive() and process_pool:
                worker.terminate()
                worker.join(2)
        if process_pool:
            target.queue.cancel_join_thread()
            target.queue.close()


if __name__ == "__main__":
    probe(True)
    probe(False)
