/*-------------------------------------------------------------------------
 *
 * monitor.h
 * 	Exports from postmaster/monitor.c.
 *
 * This is the header file for new auxiliary process for monitoring needs.
 *
 * src/include/postmaster/monitor.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * тут будуь лежать HTAB, SUbject и тд
 * либо объявление + extern, либо реализация
 * в .с - реализация
 */

#ifndef _MONITOR_H
#define _MONITOR_H
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "monitorsubsystem/monitor_channel.h"

// допустим
int max_processes = MaxBackends + max_worker_processes + autovacuum_max_workers + max_parallel_workers + 1;

#define MAX_SUBS_NUM max_processes
#define MAX_PUBS_NUM 32
#define MAX_SUBJECT_NUM 64

#define MAX_SUBS_BIT_NUM (MAX_SUBS_NUM + 64 - 1) / 64

#define MAX_SUBJECT_LEN 25

typedef enum
{
	ANYCAST,
	MULTICAST,
} routing_type;

typedef struct _subjectEnity
{
	routing_type _routingType;

	// пусть подписчики будут битовой маской
	pg_atomic_uint64 bitmap_subs[MAX_SUBS_BIT_NUM];

	// // или так, я пока не решила
	// LWLock lock;
	// uint64 bitmap[MAX_SUBS_BIT_NUM];
} SubjectEntity;

typedef struct _subjectKey
{
	char name[MAX_SUBJECT_LEN];
} SubjectKey;

typedef struct _subscriberInfo
{
	pid_t proc_pid;
	/* тут микро вопрос, как это норм задавать - возможно, лучше не через указатели, а через offset и тд... */
	monitor_channel *channel;
} SubscriberInfo;

typedef struct PublisherInfo
{
	/* мб еще лочку надо добавить */
	pid_t proc_pid;
	monitor_channel *sub_channel;
} PublisherInfo;


typedef struct MssState_SubscriberInfo
{
	LWLock lock;
	SubscriberInfo *subscribers;

	/* just in case 16, 8 might be enough */
	uint16 max_subs_num;
	uit16 current_subs_num;

} MssState_SubscriberInfo;

typedef struct MssState_PublisherInfo
{
	LWLock lock;
	PublisherInfo *publishers;

	/* just in case 16, 8 might be enough */
	uint16 max_pubs_num;
	uit16 current_pubs_num;

} MssState_PublisherInfo;



/*
 * еще раз - в разделяемой памяти лежит
 * структура (пока массив) со списком подписчиков
 * массив subjectEntities
 * хеш-мапа с соотношением subject-SubjectEntities
 * структура, где должны регистрироваться издатели
 *
 * к этим структурам должен быть доступ функциям подписаться-отписаться и тд
 * поэтому обернем все это в общую структуру с этим всем...
 *
 * возможно хеш-таблицу можно было бы и вынести отдельно, типа
 * static HTAB *mss_hash = NULL;
 * но я пока не решила
 */

/*
 * Central shared memory entry for the monitor subsystem
 *
 * SubsribersInfo, Publishers, subject-subscribers (SubjectEntities) hashtable
 * are reached from here.
 *
*/
typedef struct mssSharedState
{
	/* maybe needed someting else*/
	MssState_SubscriberInfo sub;
	MssState_PublisherInfo pub;

	SubjectEntity *subjectEntities;

	/* maybe needed*/
	// LWLock *lock; /* protects hashtable search/modification */
	HTAB *mss_hash = NULL;	/* hashtable for SubjectKey - SubjectEntity */

} mssSharedState;

/*
 * Небольшое описание, что можно было бы сделать лучше или под вопросом
 * длина subject - фиксированная
 *
 */

// I take an example from walwriter (src/backend/postmaster/walwriter.c) and other backgrounds
pg_noreturn extern void MonitoringProcessMain(char *startup_data, size_t startup_data_len);

/*
 * this needed to be included to CalculateShmemSize in src\backend\storage\ipc\ipci.c
 */
extern Size MonitorShmemSize(void);
/* */
/*
 * this is for initializing shmem for monitoring subsystem
 * this needed to be included to CreateOrAttachShmemStructs in src\backend\storage\ipc\ipci.c
 */
extern void MonitorShmemInit(void);
// каким-то боком некоорые ...ShmemInit и ..ShmemSize включены в инклюды src\backend\storage\ipc\ipci.c
// а какие то нет...
// я пока свой инклюд делать не буду, потом надо посмотреть по Makefile, как оно компилится

// extern bool PgArchCanRestart(void);
// extern void PgArchWakeup(void);
// extern void PgArchForceDirScan(void);

#endif /* _MONITOR_H */
