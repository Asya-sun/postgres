/* -------------------------------------------------------------------------
 *
 * monitor_event.h
 *	  Routines for interprocess monitoring events
 *    API for using the Monitoring Subsystem
 *
 * src/include/monitorsubsystem/monitor_event.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONITOR_EVENT_H
#define MONITOR_EVENT_H
#include "postmaster/monitor.h"

typedef enum
{
    MSS_OK,

    /* Generic error codes */
    MSS_ERR_INVALID_ARG,
    MSS_ERR_NOT_REGISTERED, 
    MSS_CHANNEL_WRONG_STATE,

    /* Connection Error Codes */
    MSS_ERR_NO_SUBJECTS_SLOTS_AVAILABLE ,
    MSS_ERR_ROUTING_MISMATCH ,
    MSS_ERR_ALREADY_SUBSCRIBED,

    /* Notify Error Codes */
    MSS_CHANNEL_BUSY,    /* Channel busy */
    MSS_NO_CHANNEL,  /* Channel is not created...(?) */
    MSS_DETACHED,       /* sender is not available (probably dead) */
} MonitorResult;


// con = Consumer / Subscriber
int pg_monitor_con_connect(MonitorChannelConfig *conConfig);
void pg_monitor_con_disconnect();

MonitorResult pg_monitor_subscribe_to_event(const char *event_string, routing_type _routing_type);
void pg_monitor_unsubscribe_from_event(const char *event_string);

// pub = Publisher
int pg_monitor_pub_connect(MonitorChannelConfig *conConfig);
void pg_monitor_pub_disconnect();

MonitorResult pg_monitor_notify(const char *event_name, const void *data, bool reliable);
#endif /* MONITOR_EVENT_H */
