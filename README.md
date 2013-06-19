What is RedFS?
==============
RedFS is an open-sourced distribution of P2P-oriented scalable distributed file synchronization/backup system. 
That is, kind of a cross of Dropbox with BitTorrent.

You can install this system on multiple computers and create your own tiny personal cloud, with the storage 
distributed between all of the peer clients (running Linux, OS X, Windows, or maybe any other OS if you find 
all the required dependencies). Such a cloud can span between multiple computers connected via public 
Internet; moreover, the clients of the system do not need to trust each other, cause even while providing 
storage space to the system, the clients won't be able to access the data of other clients, due to the 
sophisticated encryption.

Everybody is invited to join hacking RedFS, ensure its security and stability and improve it even further.


Features
========
RedFS supports (or ready to support) such features as:

* Peer-to-peer storage on untrusted client computers;
* Automatic replication of data between the client storage, to maintain the desired level of data redundancy;
* Multi-level encryption to protect the data of the clients:
 + HTTPS (with [mutual SSL authentication](http://en.wikipedia.org/wiki/Mutual_authentication)) 
   as a basic transport protocol;
 + Multi-level data encryption: 
  - cloud-level *(implemented)* — to prevent occasional data snooping, 
  - usergroup-level *(implemented)* to protect the data from unauthorized access, 
  - end-user *(planned)* — as an ultimate measure to prevent any man-in-the-middle access to the data, 
    including access by the cloud maintainer/administrator;
* Data deduplication;
* Continuous data protection (in the watched directory);
* Data synchronization between the computers of the same user.


Requirements
============
RedFS is originally written and most tested under Linux (esp. Debian/Ubuntu); the RedFS clients 
are also intended (and expected) to be executed under OS X and Windows.
It also requires a number of external packages to be installed, see [Dependencies](DEPENDENCIES.md) for details.


Overview
========
Assume an administrator planning to launch a RedFS-based cloud. What concepts and ideas should one know 
and understand, to plan such a cloud?


Cloud zones
-----------
From the maintenance point of view, the whole RedFS cloud is divided into two zones:

 * **Trusted** zone — the cloud components hosted within the reach of administrator, on controlled premises 
   (such as, within the controlled data center). It is assumed that the communication between 
   these cloud components is secured and cannot be intercepted, and unauthorized parties cannot gain access 
   to any component in the zone due to reasonable security measures.
 * **Untrusted** zone — the cloud components hosted outside the Trusted zone, such as at the personal computers 
   of third-party cloud users.


Cloud components
----------------

### Cloud components in Trusted zone

#### RelDB (relational database)
The cloud needs a relational database to function, in particular to enforce the data deduplication, 
to store the structure and to version the user contents stored in the cloud. 

**Ops note:** _requires deployment of PostgreSQL database. The amount of the data stored in the database 
  is not that huge; for example, one of the clouds running RedFS-based storage in production and 
  storing about 128 GiB of real user data, utilized the PostgreSQL database about 60 MiB large. 
  On the other hand, some (rarely executed but still crucial) queries may be pretty complex, 
  so, a powerful high-CPU server would be a good choice for deployment of the database._

**Dev note:** _SQLAlchemy ORM is used to perform the DB access; to better control the memory consumption and allow
for streaming database access, most of the DB-aware code accesses the DB using SQLAlchemy so-called
“expression API” (contrary to another one, “query API”), and this is the preferred method to programmatically
access the database from the RedFS code._
* _There may be trace amounts of the code using SQLAlchemy “query API”, most likely in non-memory-critical areas._
* _There are also noticeable volumes of legacy textual “raw SQL” queries (though still running through 
  the SQLAlchemy backend), and the strategic aim is to get rid of all of them, rewriting them to utilize 
  the “expression API”._
* _And finally, there are also several crucial spots where the PostgreSQL-specific features are used
  (not just, say, the PostgreSQL-specific functions, but most importantly — some
  PostgreSQL triggers, PL/PgSQL features, etc); for now, there are no plans to get rid of such code
  in the nearest time, due to its importance._

#### Docstore (non-relational database)
For some data with the usage profile less suitable for the relational databases, the cloud requires
a non-relational/NoSQL database deployed. Currently, it uses MongoDB for all these purposes.
**Dev note:** _the code subsystem ensuring the access to the non-relational database is internally known
as *Docstore*, meaning that it stores the “documents” rather than “relations”._

There are two significantly different scenarios how the data is stored in the non-relational database/Docstore:
either the system stores the small volumes of often-updated/often-read pieces of data, or the system stores the
significantly sized data chunks, on a kind-of “distributed Big-Data filesystem”, 
and seldom requires to write/update/read it. Luckily, MongoDB handles both scenarios pretty well, 
utilizing the general-purposes MongoDB collections for the first scenario, and the GridFS overlay 
for the second one. In terms of RedFS, the first kind of database is called _FastDB_ 
and the second one is called _BigDB_, making it possible in the future to implement different backends, 
besides MongoDB.

**Ops note:** _the usual suggestion for any MongoDB installation is to deploy the MongoDB on high-memory servers;
this, and/or setting up the MongoDB storage on a SSD, is especially useful for FastDB. The FastDB doesn't occupy 
much space in normal utilization, though it is heavier than RelDB; for example, the production cloud 
with the 128 GiB of real user data utilized about 0.5 GiB of MongoDB-based FastDB. 
The BigDB deployment is out of scope of this documentation._


#### Node
*Node* is the central component of all the cloud. Technically, it is a process running 
on a (likely, dedicated) server, having access to multiple databases: RelDB, FastDB and BigDB.
It keeps track of all the data stored in the distributed cloud, as well as all the clients currently connected
and providing the storage space.


### Cloud components in Untrusted zone
#### Host
*Host* is the name of a client-side process running, connected to the RedFS cloud (particularly, to the *Node*),
capable of both providing some space to the cloud, to synchronize/backup the data in some directory
(to ensure the continuous data protection). Each user may have multiple host processes running simultaneously
(say, on different computers) authenticated under the same username.

When running, it stores some transient information about the process of backup/synchronization, as well as about
the data being or having been backed up. It uses SQLite database for this purpose; **Dev note:**
__the data schema on the host is highly similar to one on the Trusted Zone, so most of the DB-related code
is shared between them, thanks to the SQLAlchemy ORM.__

During the synchronization of the contents from the cloud, and to store the data of the other peers, it also stores
the so-called *chunks* of the data, in a predefined directory.


History
=======
The RedFS project took more than 10 man-years to implement, with multiple strategy pivots in between.
In particular, it was multiply rebranded in the process. You may probably spot these intermediate brands
throughout the code, and you should not get too confused about it.

The very first (internal) project code name was **Calathi** (from in-team Lat. motto _Ovbus calathi_, 
meaning “(multiple) Buckets for the eggs”), accenting the idea of not storing the sensitive data 
on a single point-of-failure and distributing/replicating the data between the storage hosts.
**Dev note:** _consider “Calathi” an internal project codename, like “Chicago” or “Memphis” were for Windows OS._

Another pivotal point involved renaming the storage technology/framework behing the system to **FreeBrie**
(«free» as a beer, «brie» as a cheese), emphasizing the opportunity to join the cloud and backup
any unlimited amount of data absolutely for free… but with the tongue-in-cheek notice that you must provide
the appropriate amount of storage on your computer as well.
**Dev note:** _consider “FreeBrie” the historical name of the technology._

This technology is finally open sourced under the **RedFS** brand.
**Dev note:** _consider “RedFS” the name of the open-source project._


See also
========
* [Dependencies](DEPENDENCIES.md)
* [How-to: deploy RedFS](DEPLOY_HOWTO.md)
* [How-to: start hacking RedFS](START_HACKING_HOWTO.md)
