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
 *	  src/backend/postmaster/monitoring.c
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

// for first version of gettion some data 
#include "pgstat.h"
#include "postgres.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/ps_status.h"

#include "replication/logicallauncher.h"
#include "commands/dbcommands.h"
#include "utils/acl.h"
#include "catalog/pg_authid.h"
#include "miscadmin.h"


#include "utils/monitor_event_types.h"
#include "monitor_event.h"
#include <curl/curl.h>
#include <string.h>
#include <time.h>

// for first stats data
// define - они определены в каком-то ....c файле, поэтому их нельзя просто импортить (поэтому я вставила код сюда)
// this staff is defined in some .c file (not header), so I've got to put it here
#define UINT32_ACCESS_ONCE(var)		 ((uint32)(*((volatile uint32 *)&(var))))
#define HAS_PGSTAT_PERMISSIONS(role)	 (has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || has_privs_of_role(GetUserId(), role))


#define PORT 0x1237
#define USERS_NUMBER 5
#define BUFF_SIZE 256

#define COLLECTOR_URL	"http://localhost:4318/v1/logs"

/*
 * There sould be GUC parameters if they are needed
 */


#include <curl/curl.h>
#include <string.h>
#include <time.h>


void send_log_to_collector(const char* json_message);

void send_log_to_collector(const char* json_message) {
    CURL *curl;
    CURLcode res;
    
    curl_global_init(CURL_GLOBAL_ALL);
    curl = curl_easy_init();

    if (curl) {
        char json_payload[2048];
        struct curl_slist *headers = NULL;
        FILE *debug_file;
        long http_code = 0;
        // Экранируем JSON-сообщение для вставки
        char* escaped_json = curl_easy_escape(curl, json_message, 0);
        if (!escaped_json) {
            elog(LOG, "Failed to escape JSON message");
            curl_easy_cleanup(curl);
            curl_global_cleanup();
            return;
        }

        // Формируем финальный payload
        snprintf(
            json_payload, sizeof(json_payload),
            "{\"resourceLogs\":[{\"resource\":{},\"scopeLogs\":[{\"scope\":{},\"logRecords\":[{"
            "\"timeUnixNano\":\"%llu\","
            "\"body\":{\"stringValue\":\"%s\"},"
            "\"severityText\":\"INFO\""
            "}]}]}]}",
            (unsigned long long)time(NULL) * 1000000000,
            escaped_json  // Уже экранированная строка
        );

        // Устанавливаем заголовки
        headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");

        // Настройка CURL
        curl_easy_setopt(curl, CURLOPT_URL, COLLECTOR_URL);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json_payload);
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        
        // Для отладки (можно убрать после тестов)
        curl_easy_setopt(curl, CURLOPT_VERBOSE, 1L);
        debug_file = fopen("/tmp/curl_debug.log", "a");
        curl_easy_setopt(curl, CURLOPT_STDERR, debug_file);

        // Отправка запроса
        res = curl_easy_perform(curl);
        
        // Проверка результата
        http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        
        if (res != CURLE_OK) {
            elog(LOG, "CURL failed: %s", curl_easy_strerror(res));
        } else {
            elog(LOG, "Request completed, HTTP status: %ld, Payload: %s", http_code, json_payload);
        }

        // Очистка
        fclose(debug_file);
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        curl_free(escaped_json); 
    }
    curl_global_cleanup();
}

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

    int server_socket;
    int client_socket;
    struct sockaddr_in server_sockaddr;
    int error_check;
    int reuse = 1;
	/////////////
	int worker_number = MyProcPid;
	pgsocket socket_fd;
	struct sockaddr_un addr;
	char path[64];
	int flag = 0; 
	MonitorEvent myEvent = ME_C;
	/////////////


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

//////////////////////////////////////////////////
	elog(LOG, "[PID = %d] my number is %d\n\t\t myEvent is %d", MyProcPid, worker_number, myEvent);
	
	memset(path, 0, sizeof(path));
	snprintf(path, sizeof(path), "%s%d", "/tmp/uds_epoll_example.sock", worker_number);


	create_nonblocking_uds_socket(path, &addr, &socket_fd);
	elog(LOG, "[PID = %d] socket was created %s", MyProcPid, path);

	flag = 0;
	for (MonitorEvent event = ME_A; event < MONITOR_EVENT_NUM_TYPES; event++) {
		if (flag == 0) {
			flag = SubscribeToMonitorEvent(event, socket_fd, addr);
			if (flag == 1) {
				elog(LOG, "[PID = %d] NOT SUCCEDED SUBSCRIBING to event %d", MyProcPid, event);
			} else {

			elog(LOG, "[PID = %d] subscribed to event %d", MyProcPid, event);
			}
			
		} else {
				elog(LOG, "[PID = %d] NOT SUCCEDED SUBSCRIBING to event %d", MyProcPid, event);
		}
	}

//////////////////////////////////////////////////


    server_socket = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_socket == -1) {
        elog(ERROR, "monitoring process: error socket()");
        goto loop;
    }

    
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) == -1) {
        elog(ERROR, "monitoring process: error setsockopt()");
        close(server_socket);
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

    
    while (1) {int prev_flag = flag;
		CHECK_FOR_INTERRUPTS();

		if (!flag) {
			char message[64];
			bool error_flag = false;
			MonitorEventMessage *me_messages;
			int mnum = 0;

			memset(message, 0, sizeof(message));
			snprintf(message, sizeof(message), "%s%d", "messsage for ypu from process ", worker_number);
			
			NotifyMonitorEvent(myEvent, message, socket_fd);

			sleep(1);
			me_messages = CheckMonitorEvent(socket_fd, 1000, &error_flag, &mnum);

			if (error_flag == false) {
				elog(LOG, "[ %d ] got messages = %d", MyProcPid, mnum );
				for (int i = 0; i < mnum; i++) {
					MonitorEventMessage msg = me_messages[i];
					elog(LOG, "[ %d ] MESSAGE:\n\t\ttype = %d\n\t\tTime=%ld\n\t\tsender_pid = %d\n\t\tdata = %s",
							MyProcPid, msg.event, msg.event_time, msg.sender_pid, msg.data);
				}
				
				for (int i = 0; i < mnum; i++) {
					size_t len = 0;
					char *msg = MonitorEventMessageToJSON(&(me_messages[i]), &len);
					elog(LOG, "sending log to collector: \n%s\n", msg);
                    send_log_to_collector(msg);
					pfree(msg);
                    // send_log_to_collector(&(me_messages[i]));
				}

				// send_log_to_collector(log_data);

			} else {
				elog(LOG, "got errors during checking monitoring events, look at logs");
			}

			if (me_messages != NULL) {
				FreeMEMessagesAfterEvent(me_messages, mnum);
			}

		}
		flag = prev_flag;
		

		ereport(LOG, errmsg("test_mes_health_check"));

		sleep(3);
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
        counter += 1;
        pg_usleep( 3000L * 1000L);
    }

}

