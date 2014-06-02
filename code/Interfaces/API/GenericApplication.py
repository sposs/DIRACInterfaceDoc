from Interfaces.API.Application                     import Application
from DIRAC.Core.Workflow.Parameter                  import Parameter
from DIRAC import S_OK, S_ERROR


import types, os


class GenericApplication(Application):
    """ Run a script (python or shell) in an application environment.

    Example:

    >>> ga = GenericApplication()
    >>> ga.setScript("myscript.py")
    >>> ga.setArguments("some command line arguments")
    >>> ga.setDependency({"root":"5.26"})

    In case you also use the setExtraCLIArguments method, whatever you put
    in there will be added at the end of the CLI, i.e. after the Arguments
    """
    def __init__(self, paramdict=None):
        self.Script = None
        self.Arguments = ''
        self.dependencies = {}
        ### The Application init has to come last as if not the passed
        ### parameters are overwritten by the defaults.
        super(GenericApplication, self).__init__(paramdict)
        #Those have to come last as the defaults from Application are not right
        self._modulename = "ApplicationScript"
        self.appname = self._modulename
        self._moduledescription = 'An Application script module that can execute any provided script in the given project name and version environment'

    ###### Between here and the * symbol, the application specific setters are defined. 
    ###### Syntax is rigid: all accessors start with 'set' then the member name starting with a Capital letter
    ###### This is required due to the possibility to define any application in one line passing a dict.
    ###### See the Application class for details.
    def setScript(self, script):
        """ Define script to use

        @param script: Script to run on. Can be shell or python.
        Can be local file or LFN.
        @type script: string
        """
        #Check that the specified parameter has the right type. Not quite the right way to do this, but lightweight.
        self._checkArgs({
            'script': types.StringTypes
          })
        if os.path.exists(script) or script.lower().count("lfn:"): # add the file to the application sandbox
            self.inputSB.append(script)
            
        self.Script = script
        return S_OK()

    def setArguments(self, args):
        """ Optional: Define the arguments of the script

        @param args: Arguments to pass to the command line call
        @type args: string

        """
        self._checkArgs({
            'args': types.StringTypes
          })
        self.Arguments = args
        return S_OK()

    def setDependency(self, appdict):
        """ Define list of application you need

        >>> app.setDependency({"mokka":"v0706P08","marlin":"v0111Prod"})

        @param appdict: Dictionary of application to use: {"App":"version"}
        @type appdict: dict

        """
        #check that appdict is a python dictionary
        self._checkArgs({
            'appdict': types.DictType
          })

        self.dependencies.update(appdict)
        return S_OK()

    ##### The * symbol

    ##### Below are the internal method required. all of them need to be implemented.
    def _applicationModule(self):
        """
        This method allows to define the module parameters: application specific things
        The parameters, for ex. 'script' will become a module member. 
        """
        m1 = self._createModuleDefinition() ## This line MUST be there.
        ## Below is optional if there are no parameters. The return statement is mandatory.
        m1.addParameter(Parameter("script",      "", "string", "", "", False,
                                  False, "Script to execute"))
        m1.addParameter(Parameter("arguments",   "", "string", "", "", False,
                                  False, "Arguments to pass to the script"))
        m1.addParameter(Parameter("debug",    False,   "bool", "", "", False,
                                  False, "debug mode"))
        return m1

    def _applicationModuleValues(self, moduleinstance):
        """
        Here you set the parameter values. They are set for the module when instanciated
        An example of a complete module is given later.

        Can be 'pass' if there are no parameters.
        """
        moduleinstance.setValue("script",    self.Script)
        moduleinstance.setValue('arguments', self.Arguments)
        moduleinstance.setValue('debug',     self.Debug)

    ### Add the modules to the step: depending on the type of job: UIser Job or ProductionJob
    def _userjobmodules(self, stepdefinition):
        """
        Add the application, and run the UserJobFinalizationModule (defined in the ApplicationClass)
        """ 
        res1 = self._setApplicationModuleAndParameters(stepdefinition)
        res2 = self._setUserJobFinalization(stepdefinition)
        if not res1["OK"] or not res2["OK"]:
            return S_ERROR('userjobmodules failed')
        return S_OK()

    def _prodjobmodules(self, stepdefinition):
        """
        Add the application, and run the ComputeDataList module (defined in the Application class)
        """ 
        res1 = self._setApplicationModuleAndParameters(stepdefinition)
        res2 = self._setOutputComputeDataList(stepdefinition)
        if not res1["OK"] or not res2["OK"]:
            return S_ERROR('prodjobmodules failed')
        return S_OK()

    def _addParametersToStep(self, stepdefinition):
        """
        Add parameters to the step: those parameters are shared between modules of a step. 
        They can be accessed in the modules in the self.step_commons dictionary
        In this example, there are no extra parameters, only the base parameters (defined in Application).
        """
        res = self._addBaseParameters(stepdefinition)
        if not res["OK"]:
            return S_ERROR("Failed to set base parameters")
        return S_OK()

    def _setStepParametersValues(self, instance):
        """
        Set the set parameters values. In this example, the software dependencies 
        of this application are also added to the job software packages.
        """
        self._setBaseStepParametersValues(instance)
        for depn, depv in self.dependencies.items():
            self._job._addSoftware(depn, depv)
        return S_OK()

    ##### Consistency check
    def _checkConsistency(self):
        """ Checks that script and dependencies are set.
        """
        if not self.Script:
            return S_ERROR("Script not defined")
        elif not self.Script.lower().count("lfn:") and not os.path.exists(self.Script):
            return S_ERROR("Specified script is not an LFN and was not found on disk")

        return S_OK()

    ### Missing methods, not needed for this application.
    def _checkWorkflowConsistency(self):
        """ 
        Validate the workflow consistency: If to run an application, another must have ran before, make sure it does.
        """
        return self._checkRequiredApp() # method defined in Application

    def _resolveLinkedStepParameters(self, stepinstance):
        """
        Link the output of any other application to the input of the current.
        """
        if type(self._linkedidx) == types.IntType:
            self._inputappstep = self._jobsteps[self._linkedidx]
        if self._inputappstep:
            stepinstance.setLink("InputFile", self._inputappstep.getType(), "OutputFile")
        return S_OK() 
