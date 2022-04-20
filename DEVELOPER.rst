Developer Documentation
=======================

This document contains specific instructions for muninn developers.

General
-------
The muninn distribution consists of a single setuptools package.

The version numbering scheme for muninn is ``x.y.z`` (``.z`` is optional):

- Increase the major version number ``x`` for changes that break backward
  compatibility.
- Increase the minor version number ``y`` for changes that add new features
  without breaking backward compatibility.
- Increase the revision number ``z`` for bug fixes.


Release procedure
-----------------
For the muninn package:

- Update version number in ``muninn/__init__.py``
- Update version number in ``setup.py``
- Update version number in ``README.rst`` (3x)
- Check the list of dependencies in the ``README.rst``
- Check the upgrade instructions in ``README.rst``
- Add change history entry in ``CHANGES``
- Check that the content of ``EXTENSIONS.rst`` is up to date
  (any API changes?).
- Check copyright header (year range) in all files.
- Check that creation of the muninn package using ``python setup.py sdist``
  runs without errors.

To create the muninn package: ::

  $ python setup.py sdist

The package is now available in the ``dist`` directory.


Update API documentation
------------------------
If the Python API documentation needs to be updated, commands
similar to the following may be used: :::

  pydoc-markdown -m muninn -m muninn.archive > api.md
  python3 -m markdown --extension=extra api.md > api.html
