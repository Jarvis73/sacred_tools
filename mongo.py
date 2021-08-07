import os.path
import gridfs
import pymongo
import pymongo.errors
from datetime import datetime
import tarfile
import re

DEFAULT_MONGO_PRIORITY = 30
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


class MongoDBClient(object):
    """Initializer for MongoDBClient.

    Parameters
    ----------
    url
        Mongo URI to connect to.
    db_name
        Database to connect to.
    collection_prefix
        Prefix the runs and metrics collection,
        i.e. runs will be stored to PREFIX_runs, metrics to PREFIX_metrics.
        If empty runs are stored to 'runs', metrics to 'metrics'.
    overwrite
        _id of a run that should be overwritten.
    priority
        (default 30)
    client
        Client to connect to. Do not use client and URL together.
    failure_dir
        Directory to save the run of a failed observer to.
    """
    COLLECTION_NAME_BLACKLIST = {
        "fs.files",
        "fs.chunks",
        "_properties",
        "system.indexes",
        "search_space",
        "search_spaces",
    }
    VERSION = "FileStorageObserver-0.0.1"

    def __init__(
        self, 
        url=None, 
        db_name="sacred",
        collection_prefix="",
        overwrite=None,
        priority=DEFAULT_MONGO_PRIORITY,
        client=None,
        failure_dir=None,
        **kwargs
    ):

        if client is not None:
            if not isinstance(client, pymongo.MongoClient):
                raise ValueError(
                    "client needs to be a pymongo.MongoClient, "
                    f"but is {type(client)} instead"
                )
            if url is not None:
                raise ValueError("Cannot pass both a client and a url.")
        else:
            client = pymongo.MongoClient(url, **kwargs)

        database = client[db_name]

        if collection_prefix != "":
            # separate prefix from 'runs' / 'collections' by an underscore.
            collection_prefix = f"{collection_prefix}_"

        runs_collection_name = f"{collection_prefix}runs"
        metrics_collection_name = f"{collection_prefix}metrics"

        if runs_collection_name in MongoDBClient.COLLECTION_NAME_BLACKLIST:
            raise KeyError(
                f'Collection name "{runs_collection_name}" is reserved. '
                "Please use a different one."
            )

        if metrics_collection_name in MongoDBClient.COLLECTION_NAME_BLACKLIST:
            raise KeyError(
                f'Collection name "{metrics_collection_name}" is reserved. '
                "Please use a different one."
            )
        
        runs_collection = database[runs_collection_name]
        metrics_collection = database[metrics_collection_name]
        fs = gridfs.GridFS(database)
        self.initialize(
            runs_collection,
            fs,
            overwrite=overwrite,
            metrics_collection=metrics_collection,
            failure_dir=failure_dir,
            priority=priority,
        )

    def initialize(
        self, 
        runs_collection,
        fs,
        overwrite=None,
        metrics_collection=None,
        failure_dir=None,
        priority=DEFAULT_MONGO_PRIORITY
    ):
        self.runs = runs_collection
        self.metrics = metrics_collection
        self.fs = fs
        if overwrite is not None:
            overwrite = int(overwrite)
            run = self.runs.find_one({"_id": overwrite})
            if run is None:
                raise RuntimeError(
                    "Couldn't find run to overwrite with _id='{}'".format(overwrite)
                )
            else:
                overwrite = run
        self.overwrite = overwrite
        self.run_entry = None
        self.priority = priority
        self.failure_dir = failure_dir

    def finished_event(
        self, run, cout, config, metrics=None, code_base_dir=None, _id=None,
    ):
        if self.overwrite is None:
            self.run_entry = {"_id": _id}
        else:
            if self.run_entry is not None:
                raise RuntimeError("Cannot overwrite more than once!")
            # TODO sanity checks
            self.run_entry = self.overwrite

        # Running info saving
        self.run_entry.update(
            {
                "experiment": run['experiment'],
                "format": self.VERSION,
                "command": run['command'],
                "host": run['host'],
                "start_time": datetime.strptime(run['start_time'], TIME_FORMAT),
                "stop_time": datetime.strptime(run['stop_time'], TIME_FORMAT),
                "config": config,
                "meta": run['meta'],
                "status": run['status'],
                "result": run['result'],
                "resources": [],
                "artifacts": [],
                "captured_out": "",
                "info": {},
                "heartbeat": datetime.strptime(run['heartbeat'], TIME_FORMAT),
            }
        )

        if code_base_dir is None:
            self.run_entry["experiment"]['sources'] = []
        else:
            # We need update source links
            self.run_entry["experiment"]["sources"] = self.save_sources(run['experiment'], code_base_dir)
        
        # insert running information
        self.insert()

        # insert captured output
        _id = self.run_entry['_id']
        cout = re.sub(r'Started run with ID "\d+"', f'Started run with ID "{_id}"', cout, count=1)
        self.save({'captured_out': cout})

        # insert metrics
        if metrics is not None and len(metrics) > 0:
            info = {}
            for key in metrics:
                query = {"run_id": _id, "name": key}
                push = {
                    "steps": {"$each": metrics[key]["steps"]},
                    "values": {"$each": metrics[key]["values"]},
                    "timestamps": {"$each": metrics[key]["timestamps"]},
                }
                update = {"$push": push}
                result = self.metrics.update_one(query, update, upsert=True)
                if result.upserted_id is not None:
                    # This is the first time we are storing this metric
                    info.setdefault("metrics", []).append(
                        {"name": key, "id": str(result.upserted_id)}
                    )
            self.save({'info': info})

        return _id

    def insert(self):
        if self.overwrite:
            return self.save()

        autoinc_key = self.run_entry.get("_id") is None
        while True:
            if autoinc_key:
                c = self.runs.find({}, {"_id": 1})
                c = c.sort("_id", pymongo.DESCENDING).limit(1)
                self.run_entry["_id"] = (
                    c.next()["_id"] + 1 if self.runs.count_documents({}, limit=1) else 1
                )
            try:
                self.runs.insert_one(self.run_entry)
                return
            except pymongo.errors.InvalidDocument as e:
                raise ObserverError(
                    "Run contained an unserializable entry."
                    "(most likely in the info)\n{}".format(e)
                )
            except pymongo.errors.DuplicateKeyError:
                if not autoinc_key:
                    raise

    def save(self, new_data={}):
        try:
            self.runs.update_one(
                {"_id": self.run_entry["_id"]}, {"$set": new_data or self.run_entry}
            )
        except pymongo.errors.AutoReconnect:
            pass  # just wait for the next save
        except pymongo.errors.InvalidDocument:
            raise ObserverError(
                "Run contained an unserializable entry." "(most likely in the info)"
            )

    def save_sources(self, ex_info, actual_base_dir):
        base_dir = ex_info["base_dir"]
        source_info = []

        for source_name, md5 in ex_info["sources"]:
            abs_path = os.path.join(base_dir, source_name)
            file = self.fs.find_one({"filename": abs_path, "md5": md5})
            if file:
                _id = file._id
            else:
                with (actual_base_dir / source_name).open('rb') as f:
                    _id = self.fs.put(f, filename=abs_path)
            source_info.append([source_name, _id])
        return source_info
