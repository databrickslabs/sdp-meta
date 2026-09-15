"""Setup file for the ``dlt-meta`` compatibility wrapper.

This distribution exists so ``pip install dlt-meta`` keeps resolving on
PyPI for v0.0.10 customers. It is a thin **redirect**: it depends on the
primary ``databricks-labs-sdp-meta`` package and ships none of the
compat code itself.

Why ship no packages
--------------------
The primary ``databricks-labs-sdp-meta`` wheel already bundles BOTH
compat surfaces at its purelib root (see the top-level ``setup.py``):

  * the ``dlt_meta`` flat re-export package (from ``compat/dlt_meta/``)
  * the ``src`` v0.0.10 re-export package (from ``compat/src/``)

If this wrapper ALSO shipped ``dlt_meta`` and ``src``, then
``pip install dlt-meta`` (which pulls in ``databricks-labs-sdp-meta`` as
a dependency) would install BOTH distributions, each writing the same
``site-packages/dlt_meta/`` and ``site-packages/src/`` files. The two
``RECORD`` manifests would both claim those files, so uninstalling
either distribution would delete files the other still needs. Shipping
them from exactly one distribution (the primary one) avoids the
collision entirely; this wrapper relies on the dependency to provide
them.
"""
import os

from setuptools import setup

try:  # setuptools >= 70.1 ships the integrated command
    from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # older setuptools delegates to the wheel package
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


_PTH_NAME = "dlt_meta.pth"


class bdist_wheel_with_pth_file(_bdist_wheel):
    """Ship ``dlt_meta.pth`` at the wheel's purelib root.

    The usual ``build_py``-based trick (copy the ``.pth`` into
    ``build_lib`` so it lands in the wheel root) does NOT work here:
    this distribution declares ``packages=[]`` (see the module
    docstring — the ``dlt_meta`` / ``src`` packages come from the
    ``databricks-labs-sdp-meta`` dependency, not this wheel), and with
    nothing to build modern setuptools skips ``build_py`` entirely, so
    the override never fires.

    Instead we hook ``bdist_wheel``, which always runs. ``write_wheelfile``
    is invoked once ``self.bdist_dir`` is fully populated and just before
    the archive is zipped from it, so dropping the ``.pth`` into
    ``bdist_dir`` there gets it into both the wheel and its ``RECORD``.
    The wheel stays pure-python (``Root-Is-Purelib: true``), so pip
    extracts the root straight into site-packages and ``site.py`` picks
    up the ``.pth`` at interpreter startup.

    Note: this is a **best-effort** auto-load convenience, not the load-
    bearing mechanism. The actual ``from src.* import …`` resolution on
    serverless DLT is carried by the real ``src`` package shipped in the
    ``databricks-labs-sdp-meta`` wheel — serverless ``%pip install`` does
    not re-trigger ``site.py``'s ``.pth`` scan, so the ``.pth`` only
    fires for a normal ``pip install`` followed by a fresh interpreter.
    The ``.pth`` does NOT collide with the primary wheel (which ships no
    ``.pth``), so it is safe to ship here.

    See:
      https://peps.python.org/pep-0491/  (Root-Is-Purelib semantics)
    """

    def write_wheelfile(self, *args, **kwargs):
        src = os.path.join(os.path.dirname(__file__) or ".", _PTH_NAME)
        dst = os.path.join(self.bdist_dir, _PTH_NAME)
        self.copy_file(src, dst, preserve_mode=0)
        super().write_wheelfile(*args, **kwargs)


setup(
    name="dlt-meta",
    version="0.1.1",
    # Match the primary package's Python floor and ceiling: pyspark
    # 3.5.5 (a transitive runtime dep) is incompatible with Python
    # 3.13's pickle changes, so cap at <3.13. Re-evaluate when pyspark
    # ships a 3.13-compatible release.
    python_requires=">=3.8, <3.13",
    install_requires=[
        # Keep this redirect on the release series that provides the legacy
        # dlt_meta and src.* compatibility surfaces.
        "databricks-labs-sdp-meta>=0.1.1,<0.2.0",
    ],
    author="Ravi Gawai",
    author_email="databrickslabs@databricks.com",
    license="Databricks License",
    description="DLT-META Framework (Compatibility wrapper - please migrate to databricks-labs-sdp-meta)",
    long_description="""
# DLT-META Compatibility Package

**DEPRECATED**: This package is a compatibility wrapper for
[databricks-labs-sdp-meta](https://pypi.org/project/databricks-labs-sdp-meta/)
(DLT-META is now SDP-META). Installing it pulls in the new package and keeps
existing `dlt_meta` / `src.*` imports working unchanged.

## Migration

Replace:
```bash
pip install dlt-meta
```

With:
```bash
pip install databricks-labs-sdp-meta
```

And migrate imports when convenient:
```python
# old (still works via this wrapper)
from src.dataflow_pipeline import DataflowPipeline
# new
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
```

All functionality is identical. This wrapper is maintained for backwards
compatibility; new features land only in `databricks-labs-sdp-meta`.

Docs: https://databrickslabs.github.io/sdp-meta/ ·
Source: https://github.com/databrickslabs/sdp-meta
    """,
    long_description_content_type="text/markdown",
    url="https://github.com/databrickslabs/sdp-meta",
    project_urls={
        "Documentation": "https://databrickslabs.github.io/sdp-meta/",
        "Source": "https://github.com/databrickslabs/sdp-meta",
        "Migration Guide": "https://databrickslabs.github.io/sdp-meta/docs/operations/migration",
    },
    # Intentionally NO packages: ``dlt_meta`` and ``src`` are provided
    # by the ``databricks-labs-sdp-meta`` dependency (see module
    # docstring). Shipping them here too would make two distributions
    # own the same site-packages files.
    packages=[],
    # ``dlt_meta.pth`` ships at the wheel's purelib root (see
    # ``bdist_wheel_with_pth_file`` above). It is a best-effort startup
    # auto-load of the ``dlt_meta`` package (which comes from the
    # dependency); the real ``src`` package in the primary wheel is what
    # actually carries ``from src.* import …`` on serverless DLT.
    #
    # NOTE: The .pth is only delivered for wheel installs — i.e.
    # ``pip install ./compat`` (which builds a wheel under PEP 517)
    # or ``pip install <built-wheel>``. ``pip install -e ./compat``
    # (editable) does NOT re-run build_py, so the .pth is not
    # installed. Maintainers running editable installs should
    # explicitly ``import dlt_meta`` in their notebooks.
    cmdclass={"bdist_wheel": bdist_wheel_with_pth_file},
    entry_points={"group_1": "run=dlt_meta:main"},
    # Must stay identical to the primary package's version classifiers so both
    # advertise the same interpreters; tests/test_packaging_metadata.py enforces it.
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
        "Development Status :: 7 - Inactive",  # Indicates deprecated
        "Intended Audience :: Developers",
    ],
)
