file-pusher
===========

A simple NIO based file synchronisation daemon.

This thing simply copies files from a directory to a target directory, attempting to achieve the following goals:
  * Files are copied strictly in order of increasing mtime
  * Changes to files are detected via inotify (actually, via the Java NIO WatchService API that sits on top of it)
  * The daemon is robust to arbitrary concurrent modification to the "from" directory. However, we assume
    that the "to" directory is strictly under the control of the daemon.
  * Files and directories are copied atomically insofar as that is possible. So for example when copying
    a new file into an existing dir we copy to a temp location first and then atomically rename to replace
    any existing version, and likewise if we are copying a new directory into an existing dir.

The code is a bit messy but the tests pass!

The intended use case for this is if you have a large number of processes writing files somewhere (perhaps
a local drive) and you want to automatically make this files available in some other location (perhaps a
remote NFS shared drive), preserving the order in which files are created and not letting the remote location
ever see a half-written file.
