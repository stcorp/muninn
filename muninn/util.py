#
# Copyright (C) 2014-2022 S[&]T, The Netherlands.
#

from __future__ import absolute_import, division, print_function

import errno
import hashlib
import os
import shutil
import tempfile

from muninn._compat import string_types as basestring
from muninn._compat import path_utf8, encode, decode


class TemporaryDirectory(object):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __enter__(self):
        self._path = tempfile.mkdtemp(*self._args, **self._kwargs)
        return self._path

    def __exit__(self, exc_type, exc_value, traceback):
        shutil.rmtree(self._path)
        return False


def split_path(path):
    """Recursively apply os.path.split() to split a path into a sequence of path segments.

    Example:
        [segment for segment in split_path("/a/b/c/d.txt")] returns ["/", "a", "b", "c", "d.txt"]

    """
    head, tail = os.path.split(path)
    if not tail:
        head, tail = os.path.split(head)

    if head.endswith(os.sep):
        yield os.sep
    elif head:
        for segment in split_path(head):
            yield segment

    if tail:
        yield tail


def is_sub_path(sub_path, path, allow_equal=False):
    """Determine if a path is a sub path of (is contained in) another path.

    The 'allow_equal' flag determines whether paths that are equal are considered sub paths of eachother or not.

    Both paths are split into separate segments using os.split(), and compared segment by segment. This avoids the
    problem where "/a/bb/c" is considered a sub path of "/a/b", as would happen when comparing using str.startswith()
    or os.path.commonprefix().

    """
    sub_path_segments = [segment for segment in split_path(sub_path)]
    path_segments = [segment for segment in split_path(path)]

    if allow_equal:
        if len(sub_path_segments) < len(path_segments):
            return False
    else:
        if len(sub_path_segments) <= len(path_segments):
            return False

    for sub_path_segment, path_segment in zip(sub_path_segments, path_segments):
        if sub_path_segment != path_segment:
            return False

    return True


def make_path(path, mode=0o777):
    """Try to create the specified path, creating parent directories where needed. If the path already exists, this is
    _not_ considered an error. This is similar to "mkdir -p" and in constrast to os.makedirs(). The latter raises an
    exception if the leaf directory exists.

    Keyword arguments:
    mode -- On some systems, mode is ignored. Where it is used, the current umask value is first masked out.
            The default mode is 0777 (octal). See also the documentation of os.mkdir().

    """
    try:
        os.makedirs(path)
    except EnvironmentError as _error:
        # If the leaf directory cannot be created because an entity with the same name already exists, do not consider
        # this an error _if_ this entity is (a symbolic link to) a directory, instead of e.g. a file.
        if _error.errno != errno.EEXIST or not os.path.isdir(path):
            raise


def copy_path(source, target, resolve_root=False, resolve_links=False):
    """Recursively copy the source path to the destination path. The destination path should not exist. Directories are
    copied as (newly created) directories with the same names, files are copied by copying their contents
    (using shutil.copy()).

    Keyword arguments:
    resolve_root -- If set to True and if the top-level file/directory for the source tree is a symbolic link then the
                    source tree is copied by copying the linked target to the destination tree.
                    If set to False, the top-level link in the source tree is copied based on the setting of
                    the resolve_links parameter.

    resolve_links -- If set to True, all symbolic links in the source tree are copied by recursively copying the linked
                     targets to the destination tree. Note that this could lead to infinite recursion.
                     If set to False, symbolic links in the source tree are copied as (newly created) symbolic links
                     in the destination tree.

    """
    def _copy_path_rec(source, target, resolve_root, resolve_links):
        assert os.path.exists(source) or os.path.islink(source)

        # Refuse to copy into a dangling symlink.
        if os.path.islink(target) and not os.path.exists(target):
            raise IOError("target is a dangling symlink: %s" % target)

        if os.path.islink(source) and not (resolve_links or resolve_root):
            if os.path.exists(target):
                # This will fail if target is a directory, which is the intended behaviour.
                os.remove(target)

            # If target is a dangling symlink, os.path.exists() returns False and the creation of the symlink below
            # will fail, which is the intended behaviour.
            os.symlink(os.readlink(source), target)

        elif os.path.isdir(source):
            if not os.path.exists(target):
                # This will fail if target exists, which is the intended behaviour. Note that if the target is a
                # dangling symlink, the creation of the directory below will fail as well, which is intended.
                os.mkdir(target)

            for basename in os.listdir(source):
                source_path = os.path.join(source, basename)
                target_path = os.path.join(target, basename)
                # The resolve_root option should only have an effect during the initial call to _copy_path_rec().
                _copy_path_rec(source_path, target_path, False, resolve_links)

        else:
            shutil.copyfile(source, target)
            shutil.copystat(source, target)

    # If the source ends in a path separator and it is a symlink to a directory, then the symlink will be resolved even
    # if resolve_root is set to False. Disallow a root that refers to a file and has a trailing path separator.
    if source.endswith(os.path.sep):
        if not os.path.isdir(os.path.dirname(source)):
            raise IOError("not a directory: %s" % source)
        else:
            resolve_root = True

    # If the target is a directory, copy the source _into_ the target, unless the source path ends in a path separator.
    if os.path.isdir(target) and not source.endswith(os.path.sep):
        target = os.path.join(target, os.path.basename(source))

    # Perform the recursive copy.
    _copy_path_rec(source, target, resolve_root, resolve_links)


def remove_path(path):
    if not os.path.isdir(path) or os.path.islink(path):
        os.remove(path)
    else:
        shutil.rmtree(path)


def hash_string(string, hash_func):
    hash = hash_func()
    hash.update(string)
    return encode(hash.hexdigest())


def hash_file(path, block_size, hash_func):
    hash = hash_func()
    with open(path, "rb") as stream:
        while True:
            # Read a block of character data.
            data = stream.read(block_size)
            if not data:
                return encode(hash.hexdigest())

            # Update hash.
            hash.update(data)


# NB. os.path.islink() can be True even if neither os.path.isdir() nor os.path.isfile() is True.
# NB. os.path.exists() is False for a dangling symbolic link, even if the symbolic link itself does exist.
def product_hash(roots, resolve_root=True, resolve_links=False, force_encapsulation=False,
                 block_size=65536, hash_type=None):
    hash_func = getattr(hashlib, hash_type or 'sha1')

    def _product_hash_rec(root, resolve_root, resolve_links, hash_func, block_size):
        if os.path.islink(root) and not (resolve_root or resolve_links):
            # Hash link _contents_.
            return hash_string(path_utf8(os.readlink(root)), hash_func)

        elif os.path.isfile(root):
            # Hash file contents.
            return hash_file(root, block_size, hash_func)

        elif os.path.isdir(root):
            # Create a fingerprint of the directory by computing the hash of (for each entry in the directory) the hash
            # of the entry name, the type of entry (link, file, or directory), and the hash of the contents of the
            # entry.
            hash = hash_func()
            for basename in sorted(os.listdir(root)):
                hash.update(hash_string(path_utf8(basename), hash_func))

                path = os.path.join(root, basename)
                if os.path.islink(path) and not (resolve_root or resolve_links):
                    hash.update(b"l")
                elif os.path.isdir(path):
                    hash.update(b"d")
                else:
                    hash.update(b"f")

                hash.update(_product_hash_rec(path, False, resolve_links, hash_func, block_size))

            return encode(hash.hexdigest())

        else:
            raise IOError("path does not refer to a regular file or directory: %s" % root)

    if isinstance(roots, basestring):
        roots = [roots]

    if len(roots) == 1 and not force_encapsulation:
        return hash_type + ':' + decode(_product_hash_rec(roots[0], resolve_root, resolve_links, hash_func, block_size))

    hash = hash_func()
    for root in sorted(roots):
        hash.update(hash_string(path_utf8(os.path.basename(root)), hash_func))

        if os.path.islink(root) and not (resolve_root or resolve_links):
            hash.update(b"l")
        elif os.path.isdir(root):
            hash.update(b"d")
        else:
            hash.update(b"f")

        hash.update(_product_hash_rec(root, resolve_root, resolve_links, hash_func, block_size))

    return hash_type + ':' + hash.hexdigest()


def product_size(roots, resolve_root=True, resolve_links=False):
    def _product_size_rec(root, resolve_root, resolve_links):
        if os.path.islink(root) and not (resolve_root or resolve_links):
            # Use size of the symbolic link itself (instead of the size of its target).
            return os.lstat(root).st_size

        elif os.path.isfile(root):
            return os.stat(root).st_size

        elif os.path.isdir(root):
            return sum([_product_size_rec(os.path.join(root, base), False, resolve_links) for base in os.listdir(root)])

        else:
            raise IOError("path does not refer to a regular file or directory: %s" % root)

    if isinstance(roots, basestring):
        roots = [roots]

    return sum([_product_size_rec(root, resolve_root, resolve_links) for root in roots])


def quoted_list(lst, quote_text='"', join_text=", "):
    '''returns a string where all items are surrounded by quotes and joined'''
    return join_text.join([quote_text + str(x) + quote_text for x in lst])
