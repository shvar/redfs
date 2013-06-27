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

Note that at any moment, you can watch the list of available Node CLI commands by `cnode.py --help`,
and the list of available Client/Host CLI commands by `chost.py --help` (or `chost.community.py --help`,
more on that later).


Deploying the Node
==================

Configuring the `node.conf` file
--------------------------------

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


Launching the node
------------------

After everything is configured, you can launch the Node via `cnode.py --launch`.


Adding the users
================
… and, optionally, the hosts.

Every user who wants to connect to RedFS need their own account. For authentication, the user needs
their own (unique) username, and a password; but, for better security, the raw passwords are not stored in the databases
and never transmitted.
**Dev note:** _in particular, for authentication, the regular HTTP DIGEST authentication method is used._

Thus, every user needs their own username and a digest.

To stress that it is the user who knows the password, and that the password never leaves the user side in a clean form,
the digest generation code is present only in the `chost.py` rather than in the `cnode.py`.
You must launch `./chost.py --launch <username> <password>` to generate a desired digest line.
Also, you may launch it with the `./chost.py --launch <username>`, without passing the password; the CLI will require
you to enter the password without displaying it on the screen, what is a little more secure.

Example:

    ~/redfs:$ ./chost.py --digest MyUsername MyP@SSw0rD
    For username MyUsername, the digest is 2387ca58a5845f7ecb07022506ca2d7a71e25403

After you've generated the digest, on the Node side, you can add the desired user via `cnode.py --add-user` command:

    cnode.py --add-user MyUsername 2387ca58a5845f7ecb07022506ca2d7a71e25403

**Dev note:** _normally, you don't need any more actions for the newly created user to properly use the system.
Though, it is often very convenient to be able to control the Host-specific credentials of the user.
Each Host refers to a separate installation of the RedFS client under the specified username on any distinct computer,
and each such Host is usually referred to by a Host UUID. If you want to bind any specific host UUID to a user,
you can do that with a command like `cnode.py --add-host <username> <host UUID>`, e.g.:
`cnode.py --add-host MyUsername 00000000-1111-0000-0000-000000000001`._


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


Configuring the `host.*.conf` file
----------------------------------
For every RedFS installation, the clients need the matching `host.*.conf` file. This file defines how to connect
to the RedFS cloud, and what features are available. It should match the appropriate `node.conf` file on the
launched Node.

Why is it called `host.*.conf` here, why is the wildcard? That's again caused by the “editions” capability of RedFS.
There might be multiple editions built from the same codebase, and each edition might have the appropriate and the
independent `host.*.conf` file. But, as currently you are assumed to use the “community” edition, you should create
the `host.community.conf` file.

But how the code decides which `host.*.conf` file to use, and which edition it is running? Actually, it is the name
of the launcher file which defines it; the [`chost.py`](/chost.py) file provided is only the universal boiler plate,
but each edition might have the separate launcher file (which may be just the the renamed symlink to the same
`chost.py` file). For the purpose of Community Edition to be self-sufficient,
the [`chost.community.py`](/chost.community.py) symlink is already available in the distribution and should be used
subsequently.

**Dev note**: _`host.*.conf` allows to share the same codebase for multiple feature-different installations/distributions.
One can create the build procedure to generate multiple distribution packages, different only on the provided
`host.*.conf` file, and the name of the launcher file._

[/etc/host.sample.conf](/etc/host.sample.conf) contains a good documented overview and a sample of settings
which should be placed in it. In particular, the following settings are especially important:

* Section `[default-node]` (contains the settings about which Node should be used by the client):
 - `uuid` — should match the UUID defined on the Node.
 - `urls` — should contain the URLs on which the running Node may be available. 
* Section `[default-log-reporting]` (contains the information how the running Clients/Hosts may report
  the errors and the problems to the Node):
 - `method` — contains either `internal` (meaning that the error logs are transferred by the internal protocol
   of the Host-Node communication), or `smtp` (the error logs are transferred by the regular email);
   
   `internal` reporting method is normally great and doesn't need any external dependencies. Though the obvious
   limitation is that it requires the messaging protocol to be working properly; if it is the messaaging subsystem
   which causes any problems, the administrator won't receive any error logs.
   
   `smtp` method doesn't have such a limitation, but requires the SMTP server to be configured to accept
    the error logs. Various details of the SMTP authorization may be configured via such settings as `smtp-recipient`,
   `smtp-server`, `smtp-starttls`, `smtp-login`, `smtp-password`.


Launching the host
------------------

Now, you need to log in to the RedFS, using the user credentials you've added during the “Adding the users” step,
particularly the username and the password. Also, you need to choose a TCP port your client will be listening
for the inbound connections. No other program should be listening on this port. 

So, let's assume you've chose the port 41000, and you are logging in as the user “MyUsername”
with the password “MyP@SSw0rD”:

    chost.community.py --init 41000 MyUsername MyP@SSw0rD
