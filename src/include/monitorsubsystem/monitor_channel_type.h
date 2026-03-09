/*-------------------------------------------------------------------------
 *
 * monitor_channel_type.h
 *	  All types of monitor channel are labeled here
 *
 * IDENTIFICATION
 *	  src/include/monitorsubsystem/monitor_channel_type.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONITOR_CHANNEL_TYPE
#define MONITOR_CHANNEL_TYPE
// #include "postgres.h"
#include "monitorsubsystem/monitor_channel.h"
#include "monitorsubsystem/monitor_channel_shm_mq.h"
/*
 * It might be better to put it another place.
 * For the start size is equal to PARALLEL_ERROR_QUEUE_SIZE
 */
#define MONITOR_MESSAGE_SHM_QUEUE_SIZE			8192

typedef enum
{
    MONITOR_CHANNEL_SHM_MQ,
} MonitorChannelType;

/* 
 * every different channel type may need diffrent options,
 * so every new channel type parameters should be placed here
 * 
 */
typedef struct MonitorChannelConfig
{
    MonitorChannelType type;
    int channel_id;
    int publisher_procno;
    int subscriber_procno;

    union {
        struct {
            Size mq_size;
        } shm_mq;
    } u;
} MonitorChannelConfig;

#define MONITOR_CHANNEL_NUM_TYPES (MONITOR_CHANNEL_SHM_MQ + 1)


extern const ChannelOps *monitor_channel_options[];

extern const ChannelOps ShmMqChannelOps;


#endif /* MONITOR_CHANNEL_TYPE */
