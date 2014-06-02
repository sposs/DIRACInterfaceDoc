'''
Inspired by the LHCbDIRAC ModuleBase class

Created on Feb 6, 2014

@author: stephanep
'''
from DIRAC                                                import gLogger, S_OK, S_ERROR
from DIRAC.Core.Security.ProxyInfo                        import getProxyInfoAsString
from DIRAC.ConfigurationSystem.Client.Helpers.Operations  import Operations
from DIRAC.WorkloadManagementSystem.Client.JobReport      import JobReport
from DIRAC.RequestManagementSystem.Client.Request         import Request
from DIRAC.RequestManagementSystem.private.RequestValidator   import gRequestValidator
#from ExtDIRAC.Core.Utilities.FileUtilities                 import fullCopy

import os, urllib, types, shutil, glob, sys
from DIRAC.Core.Utilities.Adler import fileAdler
from DIRAC.Core.Utilities.File import makeGuid

class ModuleBase(object):
    """
    Base class for all the workflow modules. This file shouldn't need changes.
    """

    def __init__(self):
        '''
        Constructor
        '''
        super(ModuleBase, self).__init__()
        self.log = gLogger.getSubLogger("ModuleBase")
        result = getProxyInfoAsString()
        if not result['OK']:
            self.log.error('Could not obtain proxy information in module environment with message:\n', result['Message'])
        else:
            self.log.info('Payload proxy information:\n', result['Value'])
        self.ops = Operations()
        self.applicationLog = ''
        self.applicationVersion = ''
        self.applicationName = ''
        self.InputData = []  #Will have to become empty list
        self.SteeringFile = ''
        self.energy = 0
        self.parametricParameters = ""
        #self.NumberOfEvents = 0
        #self.WorkflowStartFrom = 0
        self.result = S_ERROR()
        self.InputFile = []
        self.ignoremissingInput = False
        self.OutputFile = ''
        self.jobType = ''
        self.stdError = ''
        self.debug = False
        self.extraCLIarguments = ""
        self.jobID = 0
        if os.environ.has_key('JOBID'):
            self.jobID = os.environ['JOBID']
        self.eventstring = ['']
        self.excludeAllButEventString = False
        self.ignoreapperrors = False
        self.inputdataMeta = {}
        #############
        #Set from workflow object
        self.workflow_commons = {}
        self.step_commons = {}
        self.workflowStatus = S_OK()
        self.stepStatus = S_OK()
        self.isProdJob = False
        self.production_id = 0
        self.prod_job_id = 0
        self.jobName = ""
        self.request = None
        self.jobReport = None
        self.basedirectory = os.getcwd()


    #############################################################################
    def getCandidateFiles(self, outputList, outputLFNs):
        """ Returns list of candidate files to upload, check if some outputs are missing.
            
          @param outputList: has the following structure:
          [ ('outputDataType':'','outputDataSE':'','outputDataName':'') , (...) ] 
              
          @param outputLFNs: list of output LFNs for the job
                        
          @return: dictionary containing type, SE and LFN for files restricted by mask
        """
        fileInfo = {}
        for outputFile in outputList:
            if outputFile.has_key('outputFile') and outputFile.has_key('outputDataSE') and outputFile.has_key('outputDataType'):
                fname = outputFile['outputFile']
                fileSE = outputFile['outputDataSE']
                filePath = outputFile['outputDataType']
                fileInfo[fname] = {'type' : filePath, 'workflowSE' : fileSE}
            else:
                self.log.error('Ignoring malformed output data specification', str(outputFile))
        
        for lfn in outputLFNs:
            if os.path.basename(lfn) in fileInfo.keys():
                fileInfo[os.path.basename(lfn)]['lfn']=lfn
                self.log.verbose('Found LFN %s for file %s' %(lfn, os.path.basename(lfn)))
                if len(os.path.basename(lfn))>127:
                    self.log.error('Your file name is WAAAY too long for the FileCatalog. Cannot proceed to upload.')
                    return S_ERROR('Filename too long')
                if len(lfn)>256+127:
                    self.log.error('Your LFN is WAAAAY too long for the FileCatalog. Cannot proceed to upload.')
                    return S_ERROR('LFN too long')
            
        #Check that the list of output files were produced
        for fileName, metadata in fileInfo.items():
            if not os.path.exists(fileName):
                self.log.error('Output data file %s does not exist locally' % fileName)
                if not self.ignoreapperrors:
                    return S_ERROR('Output Data Not Found')
                del fileInfo[fileName]

        candidateFiles = fileInfo
        #Sanity check all final candidate metadata keys are present (return S_ERROR if not)
        mandatoryKeys = ['type', 'workflowSE', 'lfn'] #filedict is used for requests
        for fileName, metadata in candidateFiles.items():
            for key in mandatoryKeys:
                if not metadata.has_key(key):
                    return S_ERROR('File %s has missing %s' % (fileName, key))

        return S_OK(candidateFiles)  
        
    #############################################################################
    def getFileMetadata(self, candidateFiles):
        """Returns the candidate file dictionary with associated metadata.
        
        @param candidateFiles: The input candidate files dictionary has the structure:
        {'lfn':'','path':'','workflowSE':''}
           
        This also assumes the files are in the current working directory.
        @return: File Metadata
        """
        #Retrieve the POOL File GUID(s) for any final output files
        self.log.info('Will search GUIDs for: %s' %(', '.join(candidateFiles.keys())))
        pfnGUIDs = {}
        generated = []
        for fname in candidateFiles.keys():
            guid = makeGuid(fname)
            pfnGUIDs[fname] = guid
            generated.append(fname)
        pfnGUID = S_OK(pfnGUIDs)
        pfnGUID['generated'] = generated

        self.log.debug('Generated GUID(s) for the following files ', ', '.join(pfnGUID['generated']))

        for pfn, guid in pfnGUID['Value'].items():
            candidateFiles[pfn]['GUID'] = guid
        
        #Get all additional metadata about the file necessary for requests
        final = {}
        for fileName, metadata in candidateFiles.items():
            fileDict = {}
            fileDict['LFN'] = metadata['lfn']
            fileDict['Size'] = os.path.getsize(fileName)
            fileDict['Addler'] = fileAdler(fileName)
            fileDict['GUID'] = metadata['GUID']
            fileDict['Status'] = "Waiting"   
          
            final[fileName] = metadata
            final[fileName]['filedict'] = fileDict
            final[fileName]['localpath'] = '%s/%s' % (os.getcwd(), fileName)  
        
        gLogger.verbose("Full file dict", str(final))
        
        #Sanity check all final candidate metadata keys are present (return S_ERROR if not)
        mandatoryKeys = ['GUID', 'filedict'] #filedict is used for requests (this method adds guid and filedict)
        for fileName, metadata in final.items():
            for key in mandatoryKeys:
                if not metadata.has_key(key):
                    return S_ERROR('File %s has missing %s' % (fileName, key))
        
        return S_OK(final)
    
    #############################################################################
    def setApplicationStatus(self, status, sendFlag=True):
        """Wraps around setJobApplicationStatus of state update client
        """
        if not self.jobID:
            return S_OK('JobID not defined') # e.g. running locally prior to submission
    
        self.log.verbose('setJobApplicationStatus(%s,%s)' % (self.jobID, status))
    
        if self.workflow_commons.has_key('JobReport'):#this should ALWAYS be true
            self.jobReport  = self.workflow_commons['JobReport']
    
        if not self.jobReport:
            return S_OK('No reporting tool given')
        jobStatus = self.jobReport.setApplicationStatus(status, sendFlag)
        if not jobStatus['OK']:
            self.log.warn(jobStatus['Message'])
    
        return jobStatus
        
    def _getRequestContainer( self ):
        """ just return the Request reporter (object)
        """
    
        if self.workflow_commons.has_key( 'Request' ):
            return self.workflow_commons['Request']
        else:
            request = Request()
            self.workflow_commons['Request'] = request
            return request
    #############################################################################
    
    def _getJobReporter( self ):
        """ just return the job reporter (object, always defined by dirac-jobexec)
        """
    
        if self.workflow_commons.has_key( 'JobReport' ):
            return self.workflow_commons['JobReport']
        else:
            jobReport = JobReport( self.jobID )
            self.workflow_commons['JobReport'] = jobReport
            return jobReport
        
    def resolveInputVariables(self):
        """ Common utility for all sub classes, resolve the workflow parameters 
        for the current step. Module parameters are resolved directly. 
        """
        self.log.verbose("Workflow commons:", self.workflow_commons)
        self.log.verbose("Step commons:", self.step_commons)
        
        self.request = self._getRequestContainer()
        self.jobReport = self._getJobReporter()
        
        self.prod_job_id = int(self.workflow_commons["JOB_ID"])
        if self.workflow_commons.get("IS_PROD", False):
            self.production_id = int(self.workflow_commons["PRODUCTION_ID"])
            self.isProdJob = True

        self.ignoreapperrors = self.workflow_commons.get('IgnoreAppError', False)
        
        self.parametricParameters = self.workflow_commons.get("ParametricParameters", "")
          
        self.applicationName = self.step_commons.get('applicationName', "")
          
        self.applicationVersion = self.step_commons.get('applicationVersion', "")
          
        self.applicationLog = self.step_commons.get('applicationLog', "")
        
        if 'ExtraCLIArguments' in self.step_commons:
            self.extraCLIarguments = urllib.unquote(self.step_commons['ExtraCLIArguments']) 

        self.SteeringFile = self.step_commons.get('SteeringFile', "")
          
        self.jobType = self.workflow_commons.get('JobType', '')

        self.jobName = self.workflow_commons.get("JobName", "")
#         if self.workflow_commons.has_key('NbOfEvts'):
#             if self.workflow_commons['NbOfEvts'] > 0:
#                 self.NumberOfEvents = self.workflow_commons['NbOfEvts']
#         
#         if 'StartFrom' in self.workflow_commons:
#             if self.workflow_commons['StartFrom'] > 0:
#                 self.WorkflowStartFrom = self.workflow_commons['StartFrom']
        
        if 'InputFile' in self.step_commons:
            ### This must stay, otherwise, linking between steps is impossible: OutputFile is a string 
            inputf = self.step_commons['InputFile']
            if not type(inputf) == types.ListType:
                if len(inputf):
                    inputf = inputf.split(";")
                else:
                    inputf = []
            self.InputFile = inputf
        
        self.ignoremissingInput = self.step_commons.get('ForgetInput', False)
                
        if 'InputData' in self.workflow_commons:
            inputdata = self.workflow_commons['InputData']
            if not type(inputdata) == types.ListType:
                if len(inputdata):
                    self.InputData = inputdata.split(";")
                    self.InputData = [x.replace("LFN:","") for x in self.InputData]
          
        if 'ParametricInputData' in self.workflow_commons:
            paramdata = self.workflow_commons['ParametricInputData']
            if not type(paramdata) == types.ListType:
                if len(paramdata):
                    self.InputData = paramdata.split(";")
        
        if not self.OutputFile:
            self.OutputFile = self.step_commons.get("OutputFile", "")
        
        #Next is also a module parameter, should be already set
        if "debug" in self.step_commons:
            if self.debug or self.step_commons.get('debug'):
                self.debug = True

        res = self.applicationSpecificInputs()
        if not res['OK']:
            return res
        return S_OK('Parameters resolved')
    
    def applicationSpecificInputs(self):
        """ Method overwritten by sub classes. Called from the above.
        """
        return S_OK()
    
    def execute(self):
        """ The execute method. This is called by the workflow wrapper when the module is needed
        Here we do preliminary things like resolving the application parameters, and getting a dedicated directory
        """
        workdir = os.path.join(self.basedirectory, self.step_commons["STEP_DEFINITION_NAME"])
        if not os.path.exists(workdir):
            try:
                os.makedirs( workdir )
            except OSError, e:
                self.log.error("Failed to create the work directory :", str(e))
        
        #now go there
        os.chdir( workdir )
        self.log.verbose("We are now in ", workdir)
        
        result = self.resolveInputVariables()
        if not result['OK']:
            self.log.error("Failed to resolve input variables:", result['Message'])
            return result
        
        if self.InputFile:
            ##Try to copy the input file to the work fdfir
            for inf in self.InputFile:
                bpath = os.path.join(self.basedirectory, inf)
                if os.path.exists(bpath):
                    try:
                        shutil.move(bpath, "./"+inf)
                    except EnvironmentError, why:
                        self.log.error("Failed to get the file:", str(why))
        
              
        if self.SteeringFile:
            bpath = os.path.join(self.basedirectory, os.path.basename(self.SteeringFile))
            if os.path.exists(bpath):
                try:
                    shutil.move(bpath, "./"+os.path.basename(self.SteeringFile))
                except EnvironmentError, why:
                    self.log.error("Failed to get the file:", str(why))
                    
            if os.path.exists(os.path.basename(self.SteeringFile)):
                self.log.verbose("Found local copy of %s" % self.SteeringFile)
        
        
        if os.path.isdir(os.path.join(self.basedirectory, 'lib')):
            try:
                shutil.copytree(os.path.join(self.basedirectory, 'lib'), './lib')
            except EnvironmentError, why:
                self.log.error("Failed to get the lib directory:", str(why))
        
        #if "Required" in self.step_commons:
        #    reqs = self.step_commons["Required"].rstrip(";").split(";")
        #    for reqitem in reqs:
        #        if not reqitem:
        #            continue
        #        if os.path.exists(reqitem):
        #            #file or dir is already here
        #            continue
        #        res = fullCopy(self.basedirectory, "./", reqitem)
        #        if not res['OK']:
        #            self.log.error("Failed to copy %s: " % reqitem, res['Message'])
        #            return res
        #        self.log.verbose("Copied to local directory", reqitem)
            
        
        
        try:
            self.applicationSpecificMoveBefore()    
        except EnvironmentError, e:
            self.log.error("Failed to copy the required files", str(e))
            return S_ERROR("Failed to copy the required files%s" % str(e))
        
        before_app_dir = os.listdir(os.getcwd())
        
        appres = self.runIt()
        if not appres["OK"]:
            self.log.error("Somehow the application did not exit properly")
        
        ##Try to move things back to the base directory
        if self.OutputFile:
            for ofile in glob.glob("*"+self.OutputFile+"*"):
                try:
                    shutil.move(ofile, os.path.join(self.basedirectory, ofile))
                except EnvironmentError, why:
                    self.log.error('Failed to move the file back to the main directory:', str(why))
                    appres = S_ERROR("Failed moving files")
              
        if os.path.exists(self.applicationLog):
            try:
                shutil.move("./"+self.applicationLog, os.path.join(self.basedirectory, self.applicationLog))
            except EnvironmentError, why:
                self.log.error("Failed to move the log to the basedir", str(why))
          
        try:
            self.applicationSpecificMoveAfter()
        except EnvironmentError, e:
            self.log.warn("Failed to move things back, next step may fail")
          
        #now move all the new stuff that wasn't moved before
        for item in os.listdir(os.getcwd()):
            if item not in before_app_dir and item != os.path.basename(self.SteeringFile) and not os.path.isdir(item):        
                try:
                    shutil.move("./" + item, os.path.join(self.basedirectory, item) )
                except EnvironmentError, why:
                    self.log.error("Failed to move the file %s to the basedir" % item, str(why))
            
        #move the InputFile back too if it's here
        for inf in self.InputFile:
            localname = os.path.join("./", os.path.basename(inf))
            if os.path.exists(localname):
                try:
                    shutil.move(localname, os.path.join(self.basedirectory, os.path.basename(inf)))
                except EnvironmentError, why:
                    self.log.error("Failed to move the input file back to the basedir", str(why))
          
        ##Now we go back to the base directory
        os.chdir(self.basedirectory)
        
        self.log.verbose("We are now back to ", self.basedirectory)
        self.listDir()
        
        return appres
    
    def listDir(self):
        """ List the current directories content
        """
        ldir = os.listdir(os.getcwd())
        self.log.verbose("Base directory content:", "\n".join(ldir))
    
    def runIt(self):
        """ Dummy call, needs to be overwritten by the actual applications
        """
        return S_OK()
    
    def applicationSpecificMoveBefore(self):
        """ If some application need specific things: Marlin needs the GearFile from Mokka
        """
        return S_OK()
    
    def applicationSpecificMoveAfter(self):
        """ If some application need specific things: Marlin needs send back its output
        """
        return S_OK()
    
    def finalStatusReport(self, status):
        """ Catch the resulting application status, and return corresponding workflow status
        """
        message = '%s %s Successful' % (self.applicationName, self.applicationVersion)
        if status:
            self.log.error( "==================================\n StdError:\n" )
            self.log.error( self.stdError )
            message = '%s exited With Status %s' % (self.applicationName, status)
            self.setApplicationStatus(message)
            self.log.error(message)
            if not self.ignoreapperrors:
                return S_ERROR(message)
        else: 
            self.setApplicationStatus('%s %s Successful' % (self.applicationName, self.applicationVersion))
        return S_OK(message)    
    
    #############################################################################
    
    def generateFailoverFile( self ):
        """ Retrieve the accumulated reporting request, and produce a JSON file that is consumed by the JobWrapper
        """
        reportRequest = None
        result = self.jobReport.generateForwardDISET()
        if not result['OK']:
            self.log.warn( "Could not generate Operation for job report with result:\n%s" % ( result ) )
        else:
            reportRequest = result['Value']
        if reportRequest:
            self.log.info( "Populating request with job report information" )
            self.request.addOperation( reportRequest )
        
        accountingReport = None
        if self.workflow_commons.has_key( 'AccountingReport' ):
            accountingReport = self.workflow_commons['AccountingReport']
        if accountingReport:
            result = accountingReport.commit()
            if not result['OK']:
                self.log.error( "!!! Both accounting and RequestDB are down? !!!" )
                return result
        
        if len( self.request ):
            isValid = gRequestValidator.validate( self.request )
            if not isValid['OK']:
                raise RuntimeError( "Failover request is not valid: %s" % isValid['Message'] )
            else:
                requestJSON = self.request.toJSON()
                if requestJSON['OK']:
                    self.log.info( "Creating failover request for deferred operations for job %d" % self.jobID )
                    request_string = str( requestJSON['Value'] )
                    self.log.debug( request_string )
                    # Write out the request string
                    fname = '%s_%s_request.json' % ( self.production_id, self.prod_job_id )
                    jsonFile = open( fname, 'w' )
                    jsonFile.write( request_string )
                    jsonFile.close()
                    self.log.info( "Created file containing failover request %s" % fname )
                    result = self.request.getDigest()
                    if result['OK']:
                        self.log.info( "Digest of the request: %s" % result['Value'] )
                    else:
                        self.log.error( "No digest? That's not sooo important, anyway: %s" % result['Message'] )
                else:
                    raise RuntimeError( requestJSON['Message'] )
    
    
    def redirectLogOutput(self, fd, message):
        """Catch the output from the application
        """
        sys.stdout.flush()
        if message:
            print message
        if self.applicationLog:
            log = open(self.applicationLog, 'a')
            log.write(message+'\n')
            log.close()
        else:
            self.log.error("Application Log file not defined")
        if fd == 1:
            self.stdError += message      
