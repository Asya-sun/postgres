#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "executor/executor.h"
#include "postmaster/bgworker.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "tcop/tcopprot.h"

#include "monitor_event.h"
#include "utils/monitor_event_types.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include "postgres.h"
#include "fmgr.h"



char *message = "hello, 2world!";
static bool hello_logs = true;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(hello_cworld);
static void hello_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void hello_start_worker(void);
PGDLLEXPORT void hello_main(Datum main_arg);
Datum hello_2world(PG_FUNCTION_ARGS);
void create_nonblocking_uds_socket(const char *socket_path, struct sockaddr_un *addr, pgsocket *sock);

Datum
hello_2world(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(message));
}


void create_nonblocking_uds_socket(const char *socket_path, struct sockaddr_un *addr, pgsocket *sock)
{
    int sockfd;
    int ret;
	int flags;

    // Создаем UDS сокет
    sockfd = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (sockfd == -1) {
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("не удалось создать UDS сокет: %m")));
    }

    // Устанавливаем неблокирующий режим
    flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) {
        close(sockfd);
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("не удалось получить флаги сокета: %m")));
    }
    
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(sockfd);
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("не удалось установить неблокирующий режим: %m")));
    }

    // Настраиваем адрес сокета
    memset(addr, 0, sizeof(*addr));
    (*addr).sun_family = AF_UNIX;
    strncpy((*addr).sun_path, socket_path, sizeof((*addr).sun_path) - 1);

    // Удаляем старый сокет, если он существует
    unlink(socket_path);

    // Привязываем сокет к адресу
    ret = bind(sockfd, (struct sockaddr *)addr, sizeof(*addr));
    if (ret == -1) {
        close(sockfd);
        ereport(ERROR,
                (errcode_for_socket_access(),
                 errmsg("не удалось привязать UDS сокет: %m")));
    }
	*sock = sockfd;

	return;
}


void
_PG_init(void)
{
	DefineCustomBoolVariable("test_2mes.enabled",
							 "Enable sample logs on hook called.",
							 NULL,
							 &hello_logs,
							 true,
							 PGC_POSTMASTER,
							 0,
							 NULL,
							 NULL,
							 NULL);

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = hello_ExecutorStart;


	hello_start_worker();
	// hello_start_worker();
}


static void
hello_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	if (hello_logs)
		ereport(LOG, (errmsg("hello_hook: start executing query"),
					  errdetail("query: %s", queryDesc->sourceText)));
}

static void
hello_start_worker(void)
{
	BackgroundWorker worker;
	char worker_name[50];
	
	memset(worker_name, 0, sizeof(worker_name));
	snprintf(worker_name, sizeof(worker_name), "%s%d", "test_2mes healthcheck", MyProcPid);

	MemSet(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	strcpy(worker.bgw_library_name, "test_2mes");
	strcpy(worker.bgw_function_name, "hello_main");
	strcpy(worker.bgw_name, worker_name);
	strcpy(worker.bgw_type, "test_2mes healthcheck");


	RegisterBackgroundWorker(&worker);
}

void
hello_main(Datum main_arg)
{
	
    int worker_number = MyProcPid;
	pgsocket socket_fd;
	struct sockaddr_un addr;
	char path[64];
	int flag = 0; 
	MonitorEvent myEvent = ME_C;
	bool subscribed = false;

	elog(LOG, "[PID = %d] my number is %d\n\t\t myEvent is %d", MyProcPid, worker_number, myEvent);
	
	memset(path, 0, sizeof(path));
	snprintf(path, sizeof(path), "%s%d", "/tmp/uds_epoll_example.sock", worker_number);

	//pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, die);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();


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
			if (event == myEvent && flag == 0) {
				subscribed = true;
			}
			
		} else {
				elog(LOG, "[PID = %d] NOT SUCCEDED SUBSCRIBING to event %d", MyProcPid, event);
		}
	}



	//while (!ShutdownRequestPending)
	while (true)
	{
		int prev_flag = flag;
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
			} else {
				elog(LOG, "got errors during checking monitoring events, look at logs");
			}

			if (me_messages != NULL) {
				FreeMEMessagesAfterEvent(me_messages, mnum);
			}

			if (subscribed) {
				UnsubscribeFromAllMonitorEvents(MyProcPid, socket_fd);
				subscribed = false;
				elog(LOG, "[%d] UNSUBSCRIBED FROM ALL EVENTS", MyProcPid);
			}

		}
		flag = prev_flag;
		

		ereport(LOG, errmsg("test_mes_health_check"));

		sleep(3);
	}
}
