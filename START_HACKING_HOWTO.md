What do you should know before you start hacking RedFS?


High-level overview
===================


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

As a convenient addition, RedFS uses the `python-dbc` module (opensourced separately) which is capable of additional
contract checking, using the contracts defined in well-known Epydoc format; this module can be flexibly controlled
and either enable all (or some) checkings or even completely avoid all extra overhead.

But, besides the various extra verifications, the RedFS launched in debug mode is much better suited for developers.
In particular, it allows to run all RedFS Node and multiple hosts in multiple instances under the same OS account,
and even more, keeping all the data in the source code directory (to simplify data and log analysis). In debug mode,
the logging level is much more verbose, at the sufficient level of verbosity to contain all the important data,
but not too much verbose to contain each and any line logged from the code.

In other words, you are highly suggested to run the Node and the Host in the debug mode, especially if you are going
to hack/debug the source.


Major libraries
===============
For legacy reasons, RedFS uses *Twisted* library as the primary asyncronous networking backend.
For the same legacy reasons, some (non-network-related; mostly DB access) code is still blocking and synchronous.
Though, the special care is taken to perform these actions in non-reactor thread.




Coding style
============
