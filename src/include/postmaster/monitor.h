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

#ifndef _MONITOR_H
#define _MONITOR_H




//I take an example from walwriter (src/backend/postmaster/walwriter.c) and other backgrounds
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

#endif							/* _MONITOR_H */
