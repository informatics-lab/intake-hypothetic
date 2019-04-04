Quickstart
==========

``intake_hypothetic`` provides quick and easy access to stored in hypothetical NetCDF files with iris.

.. iris: https://scitools.org.uk/iris/docs/latest/

Installation
------------

To use this plugin for `intake`_, install with the following command::

   conda install -c informaticslab -c intake intake_hypothetic

.. _intake: https://github.com/ContinuumIO/intake

Usage
-----

Note that iris sources do not yet support streaming from an Intake server.

Creating Catalog Entries
~~~~~~~~~~~~~~~~~~~~~~~~

Catalog entries must specify ``driver: hypothetic``,
as appropriate.


Using a Catalog
~~~~~~~~~~~~~~~

