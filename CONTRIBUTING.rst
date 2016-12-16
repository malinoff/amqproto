============================
Contribution getting started
============================

Contributions are highly welcomed and appreciated.  Every little help counts,
so do not hesitate!

.. contents:: Contribution links
   :depth: 2


.. _submitfeedback:

Feature requests and feedback
-----------------------------

We'd like to hear about your propositions and suggestions.  Feel free to
`submit them as issues <https://github.com/malinoff/amqproto/issues>`_ and:

* Explain in detail how they should work.
* Keep the scope as narrow as possible.  This will make it easier to implement.


.. _reportbugs:

Report bugs
-----------

Report bugs for pytest in the `issue tracker <https://github.com/malinoff/amqproto/issues>`_.

If you are reporting a bug, please include:

* Your operating system name and version.
* Any details about your local setup that might be helpful in troubleshooting,
  specifically Python interpreter version,
  installed libraries and amqproto version.
* Detailed steps to reproduce the bug.


.. _fixbugs:

Fix bugs
--------

Look through the GitHub issues for bugs.  Here is a filter you can use:
https://github.com/malinoff/amqproto/labels/bug

.. _writeplugins:

Implement features
------------------

Look through the GitHub issues for enhancements.  Here is a filter you can use:
https://github.com/malinoff/amqproto/labels/enhancement

Write documentation
-------------------

amqproto could always use more documentation.  What exactly is needed?

* More complementary documentation.  Have you perhaps found something unclear?
* Documentation translations.  We currently have only English.
* Docstrings.  There can never be too many of them.
* Blog posts, articles and such -- they're all very appreciated.

You can also edit documentation files directly in the GitHub web interface,
without using a local copy.  This can be convenient for small fixes.

.. note::
    Build the documentation locally with the following command:

    .. code:: bash

        $ make docs

    The built documentation should be available in the ``docs/en/_build/``.

    Where 'en' refers to the documentation language.


.. _`pull requests`:
.. _pull-requests:

Preparing Pull Requests on GitHub
---------------------------------

.. note::
  What is a "pull request"?  It informs project's core developers about the
  changes you want to review and merge.  Pull requests are stored on
  `GitHub servers <https://github.com/malinoff/amqproto/pulls>`_.
  Once you send a pull request, we can discuss its potential modifications and
  even add more commits to it later on.

.. note:: Prerequisites
  * python 3.5
  * virtualenv `venv` in the project root (or just an activated virtualenv)
    `virtualenv -p python3.5 venv` (if you don't have `virtualenv` installed,
    install it with `pip install virtualenv` first).

There's an excellent tutorial on how Pull Requests work in the
`GitHub Help Center <https://help.github.com/articles/using-pull-requests/>`_,
but here is a simple overview:

#. Fork the
   `amqproto GitHub repository <https://github.com/malinoff/amqproto>`__.  It's
   fine to use ``amqproto`` as your fork repository name because it will live
   under your user.

#. Clone your fork locally using `git <https://git-scm.com/>`_ and create a branch::

    $ git clone git@github.com:YOUR_GITHUB_USERNAME/amqproto.git
    $ cd amqproto
    # now, to fix a bug create your own branch off "master":
    
        $ git checkout -b your-bugfix-branch-name master

    # or to instead add a feature create your own branch off "feature":
    
        $ git checkout -b your-feature-branch-name feature

   Given we have "major.minor.micro" version numbers, bugfixes will usually 
   be released in micro releases whereas features will be released in 
   minor releases and incompatible changes in major releases.

   If you need some help with Git, follow this quick start
   guide: https://git.wiki.kernel.org/index.php/QuickStart

#. Run unit tests::

   $ make unittests

#. Run integration tests::

   You need to have rabbitmq up (with management plugin enabled)
   and listening on 5672 and 15672 ports. If you have docker installed,
   we will start a rabbitmq server automatically.

    $ make integrationtests

#. Check code style compliance::

   $ make codestyle-check

#. You can now edit your local working copy.

   You can now make the changes you want and run the tests again as necessary.

#. Make sure your code is formatted properly, run::

   $ make codestyle-autoformat

   This command will change files in your repository if necessary.
   Inspect `git diff` output, make sure it didn't break your changes (generally
   it shouldn't) by running tests again.

#. Commit and push once your tests pass and you are happy with your change(s)::

    $ git commit -a -m "<commit message>"
    $ git push -u

   Make sure you add a message to ``CHANGELOG.rst``. You will be added
   automatically to ``AUTHORS`` on the next release (commit author identity
   will be used which you can set using ``git config user.name`` command).
   If you are unsure about either of these steps, submit your pull request
   and we'll help you fix it up.

#. Finally, submit a pull request through the GitHub website using this data::

    head-fork: YOUR_GITHUB_USERNAME/amqproto
    compare: your-branch-name

    base-fork: malinoff/amqproto
    base: master          # if it's a bugfix
    base: feature         # if it's a feature
