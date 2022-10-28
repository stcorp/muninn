# Developer Documentation

This document contains specific instructions for muninn developers.

##General

The muninn distribution consists of a single setuptools package.

The version numbering scheme for muninn is ``x.y.z`` (``.z`` is optional):

- Increase the major version number ``x`` for changes that break backward
  compatibility.
- Increase the minor version number ``y`` for changes that add new features
  without breaking backward compatibility.
- Increase the revision number ``z`` for bug fixes.


## Release procedure

For the muninn package:

- Update version number in ``muninn/__init__.py``
- Update version number in ``setup.py``
- Update version number in ``docs/install.md``
- Check the list of dependencies in the ``docs/install.md``
- Check the upgrade instructions in ``docs/upgrade.md``
- Add change history entry in ``CHANGES``
- Check that the content of ``docs/extensions.md`` is up to date
  (any API changes?).
- Check copyright header (year range) in all files.
- Check that creation of the muninn package using ``python setup.py sdist``
  runs without errors.

To create the muninn package:

    $ python setup.py sdist

The package is now available in the ``dist`` directory.


## Update API documentation

If the Python API documentation needs to be updated, commands
similar to the following may be used:

    pydoc-markdown -I . -m muninn -m muninn.archive > docs/api.md
    sed -i -e 's/ | //g' docs/api.md

Now add the following to the top of ``api.md``:

    ---
    layout: page
    title: Python API
    permalink: /api/
    ---

    * toc
    {:toc}
