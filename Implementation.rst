=====================
How/What to implement
=====================

This section goes into the implementation detail to benefit from this framework.

There are a few files to add/modify:

  - In Workflow/Modules
  - In Interfaces/API

Workflow modules
================

All modules inherit from a ``ModuleBase`` class. This class is used to resolve the parameters and prepare the applications to run.


API
===

You'll need to following files:

  - The Job.py file, and possibily the UserJob.py and the ProductionJob.py classes. The latter being more useful as an example to develop your own as it's very specific.
  - The Application.py file
  - The Dirac.py file

Then you'll need to extend the ``Application`` class with your own. I give all the code needed and example application in the ``code`` directory of this repository.
