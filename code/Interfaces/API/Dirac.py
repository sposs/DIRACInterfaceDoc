"""
API to use to submit jobs in the Ext VO

@since:  June 2, 2014

@author: Stephane Poss
"""
from DIRAC.Interfaces.API.Dirac                            import Dirac as dapi
from DIRAC.Core.Utilities.List                             import sortList
from DIRAC.ConfigurationSystem.Client.Helpers.Operations   import Operations

from DIRAC import S_ERROR, S_OK, gLogger
import string

__RCSID__ = "$Id: $"

COMPONENT_NAME = 'Dirac'

class Dirac(dapi):
    """Dirac is VO specific API Dirac
    
    Adding specific functionalities to the Dirac class, and implement the L{preSubmissionChecks} method
    """
    def __init__(self, withRepo = False, repoLocation = ''):
        """Internal initialization of the ExtDIRAC API.
        """
        #self.dirac = Dirac(WithRepo=WithRepo, RepoLocation=RepoLocation)
        super(Dirac, self).__init__(withRepo, repoLocation )
        #Dirac.__init__(self, withRepo = withRepo, repoLocation = repoLocation)
        self.log = gLogger
        self.software_versions = {}
        self.checked = False
        self.ops = Operations()
          
    def preSubmissionChecks(self, job, mode = None):
        """Overridden method from DIRAC.Interfaces.API.Dirac
        
        Checks from CS that required software packages are available.
        @param job: job definition.
        @param mode: submission mode, not used here. 
        
        @return: S_OK() or S_ERROR()
        """
        
        if not job.oktosubmit:
            self.log.error('You should use job.submit(dirac)')
            return S_ERROR("You should use job.submit(dirac)")
        res = self._do_check(job)
        if not res['OK']:
            return res
        if not self.checked:
            res = job._askUser()
            if not res['OK']:
                return res
            self.checked = True
        return S_OK()
      
    def checkparams(self, job):
        """Helper method
        
        Method used for stand alone checks of job integrity. Calls the formulation error checking of the job
        
        Actually checks that all input are available and checks that the required software packages are available in the CS
        @param job: job object
        @return: S_OK() or S_ERROR()  
        """
        try:
            formulationErrors = job._getErrors()
        except Exception, x:
            self.log.verbose( 'Could not obtain job errors:%s' % ( x ) )
            formulationErrors = {}
        
        if formulationErrors:
            for method, errorList in formulationErrors.items():
                self.log.error( '>>>> Error in %s() <<<<\n%s' % ( method, string.join( errorList, '\n' ) ) )
            return S_ERROR( formulationErrors )
        return self.preSubmissionChecks(job, mode = '')
    
    def retrieveRepositoryOutputDataLFNs(self, requestedStates = ['Done']):
        """Helper function
        
        Get the list of uploaded output data for a set of jobs in a repository
        
        @param requestedStates: List of states requested for filtering the list
        @type requestedStates: list of strings
        @return: list
        """
        llist = []
        if not self.jobRepo:
            gLogger.warn( "No repository is initialized" )
            return S_OK()
        jobs = self.jobRepo.readRepository()['Value']
        for jobID in sortList( jobs.keys() ):
            jobDict = jobs[jobID]
            if jobDict.has_key( 'State' ) and ( jobDict['State'] in requestedStates ):
                if ( jobDict.has_key( 'UserOutputData' ) and ( not int( jobDict['UserOutputData'] ) ) ) or \
                ( not jobDict.has_key( 'UserOutputData' ) ):
                    params = self.parameters(int(jobID))
                    if params['OK']:
                        if params['Value'].has_key('UploadedOutputData'):
                            lfn = params['Value']['UploadedOutputData']
                            llist.append(lfn)
        return llist
    
    def _do_check(self, job):
        """ Main method for checks
        @param job: job object
        @return: S_OK() or S_ERROR()
        """
        #Start by taking care of sandbox
        if hasattr(job, "inputsandbox"):
            if type( job.inputsandbox ) == list and len( job.inputsandbox ):
                found_list = False
                for items in job.inputsandbox:
                    if type(items) == type([]):#We fix the SB in the case is contains a list of lists
                        found_list = True
                        for f in items:
                            if type(f) == type([]):
                                return S_ERROR("Too many lists of lists in the input sandbox, please fix!")
                            job.inputsandbox.append(f)
                        job.inputsandbox.remove(items)
                resolvedFiles = job._resolveInputSandbox( job.inputsandbox )
                if found_list:
                    self.log.warn("Input Sandbox contains list of lists. Please avoid that.")
                fileList = string.join( resolvedFiles, ";" )
                description = 'Input sandbox file list'
                job._addParameter( job.workflow, 'InputSandbox', 'JDL', fileList, description )
              
        res = self.checkInputSandboxLFNs(job)
        if not res['OK']:
            return res
        
        #apps = job.workflow.findParameter("SoftwarePackages")
        #if apps:
        #    apps = apps.getValue()
        #    for appver in apps.split(";"):
        #        app = appver.split(".")[0].lower()#first element
        #        vers = appver.split(".")[1:]#all the others
        #        vers = string.join(vers,".")
                #res = self._checkapp(sysconf, app, vers)
                #if not res['OK']:
                #    return res
        outputpathparam = job.workflow.findParameter("UserOutputPath")
        if outputpathparam:
            outputpath = outputpathparam.getValue()
            res = self._checkoutputpath(outputpath)
            if not res['OK']:
                return res
        useroutputdata = job.workflow.findParameter("UserOutputData")
        useroutputsandbox = job.addToOutputSandbox
        if useroutputdata:
            res = self._checkdataconsistency(useroutputdata.getValue(), useroutputsandbox)
            if not res['OK']: 
                return res
        
        return S_OK()
    
#     def _checkapp(self, config, appName, appVersion):
#         """ Check availability of application in CS
#         @param config: System config
#         @param appName: Application name
#         @param appVersion: Application version
#         @return: S_OK() or S_ERROR()
#         """
#         app_version = self.ops.getValue('/AvailableTarBalls/%s/%s/%s/TarBall'%(config, appName, appVersion),'')
#         if not app_version:
#             self.log.error("Could not find the specified software %s_%s for %s, check in CS" % (appName, appVersion, config))
#             return S_ERROR("Could not find the specified software %s_%s for %s, check in CS" % (appName, appVersion, config))
#         return S_OK()
    
    def _checkoutputpath(self, path):
        """ Validate the outputpath specified for the application
        @param path: Path of output data
        @return: S_OK() or S_ERROR()
        """
        if path.find("//") > -1 or path.find("/./") > -1 or path.find("/../") > -1:
            self.log.error("OutputPath of setOutputData() contains invalid characters, please remove any //, /./, or /../")
            return S_ERROR("Invalid path")
        path = path.rstrip()
        if path[-1] == "/":
            self.log.error("Please strip trailing / from outputPath in setOutputData()")
            return S_ERROR("Invalid path")
        return S_OK()
    
    def _checkdataconsistency(self, useroutputdata, useroutputsandbox):
        """ Make sure the files are either in OutputSandbox or OutputData but not both
        @param useroutputdata: List of files set in the outputdata
        @param useroutputsandbox: List of files set in the output sandbox
        @return: S_OK() or S_ERROR()
        """
        useroutputdata = useroutputdata.split(";")
        for data in useroutputdata:
            for item in useroutputsandbox:
                if data == item:
                    self.log.error("Output data and sandbox should not contain the same things.")
                    return S_ERROR("Output data and sandbox should not contain the same things.")
        return S_OK()
    
    def checkInputSandboxLFNs(self, job):
        """ Check that LFNs in ISB exist in the FileCatalog
        @param job: job object
        @return: S_OK() or S_ERROR()
        """
        lfns = []
        inputsb = job.workflow.findParameter("InputSandbox")
        if inputsb:
            isblist = inputsb.getValue()
            if isblist:
                isblist = isblist.split(';')
                for f in isblist:
                    if f.lower().count('lfn:'):
                        lfns.append(f.replace('LFN:', '').replace('lfn:', ''))
        if len(lfns):
            res = self.getReplicas(lfns)
            if not res["OK"]:
                return S_ERROR('Could not get replicas')
            failed = res['Value']['Failed']
            if failed:
                self.log.error('Failed to find replicas for the following files %s' % string.join(failed, ', '))
                return S_ERROR('Failed to find replicas')
            else:
                self.log.info('All LFN files have replicas available')
        return S_OK()
