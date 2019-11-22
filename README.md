Hybrid-Content-Manager
======================
It aims to a be a transparent, drop-in replacement for IPython's standard filesystem-backed storage system.  
These features are useful when running IPython in environments where you either don't have access to—or don't trust the reliability of—the local filesystem of your notebook server.

Getting Started
---------------
**Prerequisites:**
 - A Python installation with `Jupyter Notebook <https://github.com/jupyter/notebook>`_ >= 4.0.

**Installation:**
 - TODO

Usage
-----
The following code snippet creates a HybridContentsManager with two directories with different content managers. 

```python
c = get_config()

c.NotebookApp.contents_manager_class = HybridContentsManager

c.HybridContentsManager.manager_classes = {
    "": FileContentsManager,
    "shared": S3ContentsManager
}

# Each item will be passed to the constructor of the appropriate content manager.
c.HybridContentsManager.manager_kwargs = {
    # Args for root S3ContentsManager.
    "": {
        "root_dir": read_only_dir
    },
    # Args for the shared S3ContentsManager directory
    "shared": {
        "access_key_id": ...,
        "secret_access_key": ...,
        "endpoint_url":  ...,
        "bucket": ...,
        "prefix": ...
    },
}

# Only allow notebook files to be stored in S3
c.HybridContentsManager.path_validator = {
    "shared": lambda path: path.endswith('.ipynb')
}
```


Testing
-------
To run unit tests, simply cd to the root directory of the project and run the command ``tox``. 
This will run all unit tests for python versions 2.7, 3.6, 3.7 and jupyter notebook versions 4, 5, and 6.
