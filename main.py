""" This script contains some useful tools

storage2mongo: Convert the data of StorageObserver to MongoDBObserver

"""

import re
import json
from pathlib import Path

import pymongo
from sacred import Experiment
from sacred.observers import MongoObserver

from mongo import MongoDBClient

HOST = ""
PORT = -1
DATABASE = ""
SRC_DIR = ""

ex = Experiment('Tools', save_git_info=False)


@ex.config
def config():
    host = HOST
    port = PORT
    database = DATABASE
    src_dir = SRC_DIR

    _id = None      # experiment id of the project


@ex.main
def storage2mongo(_config, _log):
    opt = _config

    storage_path = Path(opt['src_dir'])

    if not storage_path.exists():
        raise FileNotFoundError(f'Cannot find the directory {storage_path}')
    # the source files are saved in the directory: #id/source
    code_base_dir = storage_path / "source"
    if not code_base_dir.exists():
        code_base_dir = None

    mgo = MongoDBClient(f'{opt["host"]}:{opt["port"]}', db_name=opt["database"])

    ###################################################################
    # Convert FileStorageObserver to MongoDBObserver
    ###################################################################
    # read exp outputs
    cout = (storage_path / "cout.txt").read_text()
    # read exp config
    with (storage_path / "config.json").open() as f:
        config = json.load(f)
    # read exp metrics
    with (storage_path / "metrics.json").open() as f:
        metrics = json.load(f)
    # read exp running information
    with (storage_path / "run.json").open() as f:
        run = json.load(f)

    _id = mgo.finished_event(run, cout, config, metrics, code_base_dir, opt["_id"])
    _log.info(f"Convert finished! Experiment id: {_id}")
    with open("_id.txt", "w") as f:
        f.write(str(_id))


if __name__ == '__main__':
    ex.run_commandline()
