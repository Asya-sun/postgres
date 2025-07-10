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

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "postmaster/auxprocess.h"
#include "postmaster/interrupt.h"
#include "postmaster/monitoring.h"
#include "utils/memutils.h"

// for first version of server
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <errno.h>
#include <arpa/inet.h>

#define PORT 0x1235
#define USERS_NUMBER 5
#define BUFF_SIZE 256

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
MonitoringProcessMain(char *startup_data, size_t startup_data_len) 
{
    //here i need to do some smart stuff
    MemoryContext monitoring_context;
    int counter = 0;

    Assert(startup_data_len == 0);

    MyBackendType = B_MONITORING;
    // here might be questions about pgstat_initialize(), ReplicationSlotInitialize, etc
    // but might not!
	AuxiliaryProcessMainCommon();

    elog(LOG, "monitoring process pid = %d", MyProcPid);

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

    /*
	 * Reset some signals that are accepted by postmaster but not here
     * 
     * If in future this turns to be kind on server to give connections,
     * it should be changed.
	 */
    pqsignal(SIGCHLD, SIG_DFL);

    /*
    * Create a memory context that we will do all our work in.
    */
    monitoring_context = AllocSetContextCreate(TopMemoryContext,
											  "Monitoring Process",
											  ALLOCSET_DEFAULT_SIZES);
    MemoryContextSwitchTo(monitoring_context);



    /*
    * Later, when logic will became clearer,  maybe 
    * it would be need to set up sigsetjmp() for exception handling
    * May check e.g. WalSummarizerMain (src/backend/postmaster/walsummarizer.c)
    * or CheckpointerMain (src/backend/postmaster/checkpointer.c)
    * 
    * On the other hand, there's no such stuff at WalReceiverMain (src/backend/replication/walreceiver.c) 
    */

    /*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	sigprocmask(SIG_SETMASK, &UnBlockSig, NULL);

    int server_socket;
    int client_socket;
    struct sockaddr_in server_sockaddr;
    struct sockaddr_in client_sockaddr;
    char buffer[BUFF_SIZE];
    int error_check;

    server_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_socket == -1) {
        elog(ERROR, "monitoring process: error socket()");
        goto loop;
    }

    memset(&server_sockaddr, 0, sizeof(struct sockaddr));
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.8.2", &server_sockaddr.sin_addr);
    bzero(&(server_sockaddr.sin_zero),8);

    error_check = bind(server_socket, (struct sockaddr *) &server_sockaddr, sizeof (server_sockaddr));
    if (error_check == -1) {
        elog(ERROR, "monitoring process: error bind()");
        close(server_socket);
        goto loop;
    }

    
    while (1) {
        elog(LOG, "monitoring process: ready to hear messages");
        printf("ready to hear messages\n");
        size_t len = sizeof(struct sockaddr);
        if (recvfrom(server_socket, buffer, BUFF_SIZE, 0, (struct sockaddr *) &client_sockaddr, (socklen_t *) &len) == -1) {
            elog(ERROR, "monitoring process: error recvfrom()");
            close(server_socket);
            goto loop;
        }

        elog(LOG, "monitoring process: message from client: %s", buffer);


        if (sendto(server_socket, buffer, BUFF_SIZE, 0, (struct sockaddr *) &client_sockaddr, len) == -1) {
            elog(ERROR, "monitoring process: error sendto()");
            close(server_socket);
            goto loop;
        }

        elog(LOG, "monitoring process:message was sent to client");
    }


    close(client_socket);
    close(server_socket);

    /*
	 * Loop forever
	 */
    loop:
	for (;;)
	{
        elog(LOG, "monitoring process is working now!!! counter = %d", counter);
        // elog(LOG, "monitoring line: %d", __LINE__);
        counter += 1;
        pg_usleep( 3000L * 1000L);
    }

}
