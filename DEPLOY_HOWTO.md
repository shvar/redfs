OS dependencies
===============
To run RedFS, you need either Windows (XP and higher), OS X (10.7 and higher)
or Linux (Debian 7.0 “Wheezy”/Ubuntu 12.10 “Quantal Quetzal” or higher are suggested).

**Dev note:** _if you are planning hacking RedFS, using Linux is highly suggested.
Though there is known successful experience hacking RedFS under OS X/Windows._


Package dependencies
====================
To launch the RedFS components, as the very least, you need Python 2.7
(Python 3 is not yet supported and not even tested). There are other mandatory dependencies
on Python packages needed to launch any components of the system, such as:
* python-twisted
* python-sqlalchemy
* python-openssl
* python-crypto (python-m2crypto may go as well)
* python-dateutil 

The following packages are also recommended, though are not mandatory:
* python-sqlalchemy-ext


Trusted zone components (e.g. Node)
-----------------------------------
You need to install at least the following Python packages:
* python-tz
* python-netifaces
* python-pymongo, python-bson, python-gridfs
* python-psycopg2

The following packages are also recommended, though are not mandatory:
* python-pymongo-ext, python-bson-ext

To deploy the databases/storage, you need to install:
* PostgreSQL (9.1 or higher);
* MongoDB (2.0 or higher).


Untrusted zone components (e.g. Host)
-------------------------------------
You need to install at least the following Python packages:
* python-pyside (recommended; 1.1.1 or newer) or python-qt4 (4.6-1 or higher).

**Dev note:** _python-pyside and python-qt4 assume Qt4 library to be installed; at the moment, RedFS uses Qt4 not for the means
of cross-platform GUI, but to implement the cross-process reactor loop, capable of tracking the filesystem changes
in realtime._
