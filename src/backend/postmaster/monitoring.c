/*-------------------------------------------------------------------------
 *
 * monitoring.c
 *
 * This is prototype for special monitor system.
 * The idea is that monitoring is implemented through usual backends,
 * which means there's no way to  monitor systems without looking at logs
 * (no way to connect to db cluster),
 * so this is gonna be special process that is possible to supply statistics
 * and other monitoring data even during recovery (when db data is still inconsistent)
 * So it is what it is)
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/walwriter.c
 *
 *-------------------------------------------------------------------------
 */
// 
#include "postgres.h"

#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "postmaster/monitoring.h"

/*
 * There sould be GUC parameters if they are needed
 */


/*
 * Main entry point for monitoring process
 *
 * ???
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */


//I take an example from walwriter (src/backend/postmaster/walwriter.c) and other backgrounds
void 
MonitoringProcessMain(char *startup_data, size_t startup_data_len) {
    //here i need to do some smart stuff

    Assert(startup_data_len == 0);

    MyBackendType = B_MONITORING;
    // here might be questions about pgstat_initialize(), ReplicationSlotInitialize, etc
    // but might not!
	AuxiliaryProcessMainCommon();

    /*
    * This is time to ... with signal and signal handlers
    * There's need to fully understand how they work and (maybe) write my own signal handlers
    * and imagine how I should make them communicate
    * And make Postmaster main loop to reawake it if it was killed or whatever
    * 
    */

    /*
    * After previous task there will be need to understand what is memory context
    * and what to do to with it  
    * (what it needed for, what to do with it, should I create new one for this process, etc)
    * 
    */
   /*
    * Okay, just let it be
    */
    

    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    /*
    * SIGINT and SIGTERM are used for fast and smart shutdown
    * 
    * Actually, there's need to set up end-of-session request
    * to client, using this process connection to monitor postgres server.
    * So it's just for start, later it should be done properly.
    * 
    * There's also need to look at how it implemented at backend process,
    * I suppose it would look somehow similar
    */
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	/* SIGQUIT handler was already set up by InitPostmasterChild */
    pqsignal(SIGALRM, SIG_IGN);

	/*
    * For the first version, it's okay
    * I suppose, later it would remind some kind of backend process...
    * 
    */
    pqsignal(SIGPIPE, SIG_IGN);
    /*
    * Actually, I think it needs to look somehow another
    * but it would be changed later, I'm tired now...
    * ACTUALLY, I think it should be 
    * combination of backends and (maybe) startup
    * bc it's gonna be a mixture of background + backend
    * 
    */
    pqsignal(SIGUSR1, SIG_IGN);
	pqsignal(SIGUSR2, SIG_IGN);



}
