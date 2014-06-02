===================
Underlying concepts
===================

This chapter introduces the concepts of the `workflow` and the `applications` and a `job`.

Workflow
========

DIRAC provides a workflow mechanism. A workflow is a multi layer structure:

  - Modules: atomic element of the workflow, executes one function such as runnign an application. 
  - Steps: collection of modules grouped together in a logical unit. For example after running any application one could 
    want to run a log file analysis, or connect to a specific service.
  - Workflow: Collection of steps. 

All those elements can be connected as the `Workflow` object has the notion of `linked parameter`. In principle, any parameter can be linked, but this isn't 
fully used here. In this intrerface this feature is only used to link steps together with data files only: the input data to a step can be the output data 
of another.

The complexity in using the `workflow` object in DIRAC comes from the fact that there are many methods that need to be called to build it, in a specific order.
For instance, one needs to get a Step, then add parameters, then get a StepInstance, then set the parameters' values. This has to be done for every element
of the workflow, modules, steps, workflow. As there are many modules per step and many steps per workflow, it can be come tricky to define a workflow.


Application
===========

A fundamental question was raised when developing this interface: What is an application? It's a piece of software that: 

  - has a name
  - requires a steering file or option file to give it instructions on what to do,
  - can take some data as input (or not),
  - produces something in output,
  - has a log file,
  - uses version control: has a version (not mandatory)

In addition, application may require specific input information: files, numbers, etc. From this set of parameters, it was logical to assume that an application would be 
represented as a ``Step`` in terms of DIRAC workflow. The code running the application itself would be a ``Module``, and the steps preparing the application
and treating its output also. Therefore, an application is a collection of modules.

To ensure a minimum failure due to user interaction, a mechanism was developed to ensure consistency at submission. All applications must be equipped with a specific 
method that validates it. This is particularly useful to validate input parameters. For example, if an application requires to have the version defined, that method would
return an error before the job is submitted.


When running, every application get to run in its own directory. Files are moved across the different directories automatically. 


Job
===

What is a Job? It's a logical structure that executes a collection of applications. There are several kind of jobs: user jobs and production jobs (through the TS). The 
former have input and output sandboxes and data, and can be submitted by regular users. The latter do not have input nor output sandboxes neither do they have data, as
they are defined by the ``Transformation System``. Also, they cannot be submitted directly as they must go through the TS, and thus require to be ran by specific 
users or groups.

Nevertheless, all the jobs share properties: 

  - name
  - CPUTime
  - System config
  - etc.

Depending on the type of job and even on the application ran, several things must be done on the job. For instance, log file names can be determined automatically 
for user jobs, or data paths can be determined given production definition and/or input for production jobs.

It is therefore logical to have the following structure:



The job is a list of applications, independently of the type of job or the application, so there is a single connection point between the two. This is repredented by 
an ``append`` method that does several things: 

  - Checks the sanity of the application: all applications have a method to check their consistency. At that point, the application also knows about the jobs it's in.
  - Define the corresponding step: parameters are collected from the application
  - Add the step to the workflow


