# A Sacred Tool to Convert FileStorageObserver to MongoDBObserver

This is a simple tool to convert FileStorageObserver to MongoDBObserver in sacred (a python library used for experiment management).

## Usage

1. Modify the code source directory as you need (in `main.py`). If no source code is saved, just ignore this step.

```python
code_base_dir = storage_path / "source"
if not code_base_dir.exists():
    code_base_dir = None
```

2. Run the script.

```bash
python main.py with host=<HOST> port=<PORT> database=<DATABASE> src_dir=<SRC_DIR>
```
