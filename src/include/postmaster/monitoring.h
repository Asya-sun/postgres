/*-------------------------------------------------------------------------
 *
 * monitoring.h
 *	  Exports from postmaster/monitoring.c.
 *
 * This is the header file for new auxiliary process for monitoring needs.
 * 
 * src/include/postmaster/monitoring.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _MONITORING_H
#define _MONITORING_H

/*
 * There should be GUS options, if they would be used
 * 
 */

extern void MonitoringProcessMain(char *startup_data, size_t startup_data_len) pg_attribute_noreturn();

#endif							/* _MONITORING_H */