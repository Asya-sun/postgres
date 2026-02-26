/*-------------------------------------------------------------------------
 *
 * monitor_channel_shm_mq.h
 *	  Monitor channel based on shm_mq
 *
 * IDENTIFICATION
 *	  src/include/monitorsubsystem/monitor_channel_shm_mq.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHM_MQ_MONITOR_CHANNEL_H
#define SHM_MQ_MONITOR_CHANNEL_H
#include "postgres.h"
#include "monitorsubsystem/monitor_channel.h"
#include "storage/shm_mq.h"

typedef struct MonitorChannelConfig MonitorChannelConfig;

/* 
 * Private data for shm_mq monitor channel 
 * 
 * It's not needed at the moment, but it's apparently 
 * much easier to remove it later than to add it, 
 * so we're temporarily keeping it
 */
typedef struct ShmMqChannelData
{
	shm_mq *mq;
	/* mb bool is_sender */
} ShmMqChannelData;

typedef struct ShmMqChannelLocal
{
    shm_mq_handle *handle;
} ShmMqChannelLocal;


bool shm_mq_channel_init(monitor_channel *ch, MonitorChannelConfig *arg);

ChannelOpResult shm_mq_channel_send_msg(monitor_channel *ch, const void *data, Size len, bool nowait);

ChannelOpResult shm_mq_channel_receive_msg(monitor_channel *ch, void *buf, Size buf_size, Size *out_len);

void shm_mq_channel_cleanup(monitor_channel *ch);

ChannelOpResult shm_mq_channel_attach(monitor_channel *ch);

void shm_mq_channel_detach (monitor_channel *ch, void *local);

extern const ChannelOps ShmMqChannelOps;

#endif /* SHM_MQ_MONITOR_CHANNEL_H */