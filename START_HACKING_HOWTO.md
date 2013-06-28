What do you should know before you start hacking RedFS?


High-level overview
===================
The Python packages comprising the RedFS codebase are these:
* [`common`](/common) — the most general code, available at any component of the RedFS.

* [`protocol`](/protocol) — the messages for the communication between the Nodes and the Hosts in the RedFS.

* [`trusted`](/trusted) — the code specific for the “Trusted side” of the RedFS, i.e. for the components running
  under the control of the RedFS cloud maintainer.

  In particular, contains the RelDB ([`/trusted/db/`](/trusted/db/))
  and the FastDB/BigDB ([`/trusted/docstore/`](/trusted/docstore/)) access code.

 - [`node`](/node) — the main of the “Trusted side” components, the Node.

 - [`node_cli`](/node_cli) — the CLI frontend for the Node.

* [`host`](/host) — the code to support the Host-specific process.

  In particular, contains the code to access the Host's embedded SQLite database ([`/host/db/`](/host/db/),
  though it may some day move to `/untrusted/db/` or `/uhost/db/`).

  Note: it is a “top level” package in the hierarchy rather than, say, an Untrusted-specific one, due to the fact
  that the Host may technically be executed on the Trusted-side as well.
  See the [`uhost`](/uhost) explanation below.

* [`untrusted`](/untrusted) — the code specific for the “Untrusted side” of the RedFS, i.e. running on the premises
  of RedFS cloud users and, probably, inaccessible by the RedFS cloud maintainer.

 - [`uhost`](/uhost) — the Host process, specifically when running on the Untrusted side; i.e., the regular Host
   running at the premises of the client.

- [`uhostcli`](/uhostcli) — the CLI frontend for the Untrusted Host.

Also, you may generate the extra documentation using [Epydoc](http://epydoc.sourceforge.net/).


Debug mode
==========
For better debugging, RedFS heavily relies on Python-s “debug mode”
(i.e. launching the Python interpreter without `python -O` or `python -OO`).
If you are not aware, launching the code in the “optimized”, non-debug mode causes just the following changes
in the normal code flow comparing to launching the code without `python -O` or `python -OO`:
 1. The builtin `__debug__` constant gets assigned `False` instead of `True`
   * all the conditional branches testing for `__debug__ == False` (or equivalent) are byte-code-compiled without
     such a test (cause it is `False` anyway);
   * all the conditional branches testing for `__debug__ == True` (or equivalent) are completely eliminated during
     the byte-code compilation phase.
 2. as a consequence, all the `assert` statements (which, in its `assert expression1, expression2` form, 
    are totally equivalent to `if __debug__: if not expression1: raise AssertionError(expression2)`) get eliminated
    from the code.

As a convenient addition, RedFS uses the [`python-dbc`](/contrib/dbc) module
([opensourced separately](https://code.google.com/p/python-dbc/))
which is capable of additional contract checking, using the contracts defined in well-known
[Epydoc](http://epydoc.sourceforge.net/) format;
this module can be flexibly controlled and either enable all (or some) checkings or even completely avoid
all extra overhead.

But, besides the various extra verifications, the RedFS launched in debug mode is much better suited for developers.
In particular, it allows to run all RedFS Node and multiple hosts in multiple instances under the same OS account,
and even more, keeping all the data in the source code directory (to simplify data and log analysis). In debug mode,
the logging level is much more verbose, at the sufficient level of verbosity to contain all the important data,
but not too much verbose to contain each and any line logged from the code.

In other words, you are highly suggested to run the Node and the Host in the debug mode, especially if you are going
to hack/debug the source.


Major libraries
===============
For legacy reasons, RedFS uses [Twisted](http://twistedmatrix.com/) library as the primary asyncronous
networking backend.
For the same legacy reasons, some (non-network-related; mostly DB access) code is still blocking and synchronous.
Though, the special care is taken to perform these actions in non-reactor thread.

For the main loop, Twisted uses qt4reactor (and thus, accordingly, [Qt4](http://qt-project.org/) and
its [PySide](http://qt-project.org/wiki/PySide) bindings).
PySide bindings are used due to their more lax licensing
than [PyQt4](http://www.riverbankcomputing.co.uk/software/pyqt/) (though ther latter might work as well). 
The only reason why qt4reactor is used (instead of, say, epollreactor on Linux, or other OS-specific reactors),
is due to the Qt4 feature of realtime directory changes tracking.

[SQLAlchemy](http://www.sqlalchemy.org/) is used to perform the DB access;
to better control the memory consumption and allow for streaming database access,
most of the DB-aware code accesses the DB using SQLAlchemy so-called
“[expression API](http://docs.sqlalchemy.org/en/latest/core/expression_api.html)”
(contrary to another one, “[ORM query API](http://docs.sqlalchemy.org/en/latest/orm/query.html)”),
and this is the preferred method to programmatically access the database from the RedFS code._
* There may be trace amounts of the code using SQLAlchemy “ORM query API”,
  most likely in non-memory-critical areas.
* There are also noticeable volumes of legacy textual “raw SQL” queries (though still running through 
  the SQLAlchemy backend), and the strategic aim is to get rid of all of them, rewriting them to utilize 
  the “expression API”.
* And finally, there are also several crucial spots where the PostgreSQL-specific features are used
  (not just, say, the PostgreSQL-specific functions, but most importantly — some
  PostgreSQL triggers, PL/PgSQL features, etc); for now, there are no plans to get rid of such code
  in the nearest time, due to its importance.



Coding style
============
The existing coding style may be considered a little bit quirky, though it is mostly based on the
[PEP-8](http://www.python.org/dev/peps/pep-0008/) and the 
[Google Python Style Guide](http://google-styleguide.googlecode.com/svn/trunk/pyguide.html), with several
additions/corrections, such as:

* Blank lines:
 - **1 blank line** can be used inside any functions, to split the logical sections
   (PEP8 quote: _use blank lines in functions, sparingly, to indicate logical sections._).
 - Separate the methods and the functions with **2 blank lines** (using 1, as PEP-8/Google Python Style Guide suggests,
   makes it possible to confuse them with the logical sections inside a function). 2 blank lines to separate
   the functions (including the inlined functions), 1 blank line to separate inside the function.
 - Separate the classes, as well as the major logical groups inside the module, by 3 blank lines.
* Alignment:
 - If something spans multiple lines in a way like a _list_ (so each line takes the same kind of entities),
   it is preferred to **align them**. E.g.:

       ```python
       logger.debug('This is a pretty long, spanning multiple lines, text string '
                     'that looks pretty much verbose and contains enough information '
                     'to understand the code flow in this place, if any problem occurs')
       ```

 - If something spans multiple lines so that the subsequent lines _continue_ the first line,
   rather than enumerate the subsequent entities for the entity on the first line,
   **indent 4 spaces from the alignment point** so that the aligned entities do not look or feel
   like the list items. Further indendations on the next lines are not necessary. E.g.:

       ```python
       variable = [some_function_call(item)
                        for item in items
                        if item.satisfies(condition)]
       ```
   rather than
       ```python
       variable = [some_function_call(item)
                    for item in items
                    if item.satisfies(condition)]  # from the first glance, looks too much like a list
       ```
   The variant like below may be also useful sometimes:

       ```python
       variable = [some_function_call(item)
                        for item in items
                            if item.satisfies(condition)
                        for items in item_sets
                            if condition_checker(items)]
       ```

   A special dislike is when the continuation line starts _to the left_ (i.e., with negative indentation)
   from the line it continues, e.g.:

       ```python
       variable = [some_function_call(item)
           for item in items
           if item.satisfies(condition)]  # from the first glance, looks too much like a list
       ```


In other words, most general suggestion: **treat the code as 2D blocks, rather then 1D wrapped lines.**
