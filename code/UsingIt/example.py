#!/bin/env python

if __name__=="__main__":
    #magic lines
    from DIRAC.Core.Base import Script
    Script.parseCommandLine()
    
    #logger and stuff
    from DIRAC import gLogger, exit as dexit

    #here, the fun things start    
    from Interfaces.API.UserJob            import UserJob
    from Interfaces.API.GenericApplication import GenericApplication
    from Interfaces.API.Dirac              import Dirac
    
    #Get a dirac instance
    d = Dirac(True, "jobrep.rep")
    
    #define your applications
    gen_app1 = GenericApplication()
    gen_app1.setScript("hello.sh")
    gen_app1.setArguments('something or another')
    gen_app1.setOutputFile("something.ext")
    
    gen_app2 = GenericApplication()
    gen_app2.setScript("something.sh")
    gen_app2.setOutputFile("other.ext")
    
    #define the job
    j= UserJob()
    j.setName("DummyJob")
    j.setCPUTime(1000)
    
    res = j.append(gen_app1)
    if not res['OK']:
        gLogger.error(res['Message'])
        dexit(1)
    res = j.append(gen_app2)
    if not res['OK']:
        gLogger.error(res['Message'])
        dexit(1)
    j.setOutputSandbox(["*.log","something.ext", "other.ext"])
    
    #submit it
    res = j.submit(d)
    if not res['OK']:
        gLogger.error(res['Message'])
        dexit(1)
    dexit(0)