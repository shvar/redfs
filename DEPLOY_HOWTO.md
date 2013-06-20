How can you deploy your private RedFS cloud?


Before deploying
================
Make sure you've learnt the basics and have read at least the overall RedFS [Readme](README.md).

Make sure you have installed all the required [Dependencies](DEPENDENCIES.md).
And, obviously, you have cloned the latest code from the [redfs.org](http://redfs.org).

Have you deployed a PostgreSQL database for RelDB, and MongoDB/GridFS for FastDB/BigDB?
If you've done everything according to the procedure so far, you already know what do “RelDB”, “FastDB” and “BigDB”
mean.

Then, you are suggested to decide whether you want to launch the system in the debug mode or in the release mode
(see [the “start hacking” howto](START_HACKING_HOWTO.md#debug-mode) for details). You are highly suggested to launch
it in the debug mode. The following explanation assumes it.


Deploying the Node
==================
Before launching the Node, create the `node.conf` configuration file. When running in debug mode,
it should be placed in the `etc/` directory inside the codebase; in release mode, it will be system-wide stored
inside `/etc/freebrie`.

[/etc/node.sample.conf](/etc/node.sample.conf) contains a good documented overview and a sample of settings
which should be placed in it.

Among the mandatory settings to be mentioned in the `node.conf`, are following:

 * `edition` — the RedFS codebase supports multiple product editions, which may differ by the set of supported features.
   You should use the `community` value for it.
 * `ssl-cert`, `ssl-pkey` — put here the (relative) path to the SSL Certificate and SSL Private Key of the Node.
   The Node will use this Certificate/Private Key during the mutual authentication with the connecting Hosts.
   The current distribution contains the sample certificate and keys generated for the Node with 
   UUID `00000000-0000-2222-0000-000000000001`.
 * `required-version`, `recommended-version` — the RedFS supports tracking the mismatches between the versions of the
   Node and of the connected Hosts and may suggest (or require) some Host to be upgraded to the latest version
   to be able to access the RedFS system. On the Host side, the version is stored in the
   [`/etc/build_version`](/etc/build_version) file, and may be changed by the build scripts.
   For the regular deployment, you could set `required-version=1`, `recommended-version=1`.
 * `rel-db-url`, `fast-db-url`, `big-db-url` — set them to the credentials of your RelDB/FastDB/BigDB deployments
   (the required format is described in [`node.sample.conf`](/etc/node.sample.conf)).
   RelDB credentials should permit the role not only to access the according PostgreSQL database for read and write,
   but also to be able to create/modify the data schema (at least, during the very first Node launch or during the first
   Node relaunch after the Node codebase is updated).

  *Ops note:* _the very first launch of the Node automatically
   creates the data schema in the PostgreSQL DB; if the codebase is updated and contains some alterations
   to the data schema, during the first relaunch of the Node after the codebase update, the data schema
   will be migrated. Otherwise, the RedFS doesn't change the data schema during the regular functioning; you may freely
   revoke the data schema alterations rights from the PostgreSQL role between such maintenance periods._
 * `[node-###]` sections — each section refers to a separate Node process running on the same configuration.
   For the regular deployment, you better leave only a single section. The UUID of the Node you specify in this section
   should be unique in the cloud, and must correspond to the one mentioned in the SSL Certificate.

After everything is configured, you can launch the Node via `cnode.py --launch`.

Also note that, at any moment, you can watch the list of available Node CLI commands by `cnode.py --help`.


Adding the users
================
… and, optionally, the hosts.




Deploying the Client(s)/Host(s)
===============================
As with the Node, before deploying you should decide whether you want a “debug” deployment or a “release” one.
For the sake of explanation simplicity (and as a default), let's assume you are using the debug deployment as well.

**Ops note:** _besides the extra checks and logging levels, the “debug” deployment significantly differs by the location of
the per-host data. This data include:_
* _The directory to store the error/debug logs (when in the debug mode, they may occupy quite a storage):_
 - _In the debug mode, the logs will be generated right in the directory with the launcher `chost.py` file._
 - _In the release mode, the logs will go to the “dot-projectname” directory in your home/user directory
   (for now, the logs for the Community Edition will go into `~/.freebrie`)._
* _The directory being tracked for changes/synchronized/backed up._
 - _In the debug mode, the directory is will be located right in the directory with
   the launcher `chost.py` file, and be named like `.sync-<HOST UUID>`
   (for example, `.sync-00000000-1111-0000-0000-000000000001`)._
 - _In the release mode, the synchronized directory will be located in your home/user directory and
   named after the project edition (for now, the logs for the Community Edition will go into `~/.freebrie`)._
* _The directory with the chunk storage (where the chunks from the peer users, and the chunks needed to
  restore/replicate the information, are placed)._
 - _In the debug mode, this directory is will be located right in the directory with
   the launcher `chost.py` file, and be named like `.host-<HOST UUID>`
   (for example, `.host-00000000-1111-0000-0000-000000000001`)._
 - _In the release mode, this directory will be named “`chunks`” and located in the “dot-projectname” directory
   in your home/user directory. For example, the chunks for the Community Edition will be located in  
   in `~/.freebrie/chunks`._
