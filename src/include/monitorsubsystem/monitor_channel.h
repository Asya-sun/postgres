/*-------------------------------------------------------------------------
 *
 * monitor_channel.h
 *	  Api of monitor channel, used in monitor Subsystem to deliver messages to consumers
 *
 * Channels for monitoring susbsystem must be created by publishers
 * and subscribers-processes, not by monitor process
 * 
 * IDENTIFICATION
 *	  src/include/monitorsubsystem/monitor_channel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONITOR_CHANNEL
#define MONITOR_CHANNEL
#include "postgres.h"

#define CH_ATTACH_ACTIVE \
    (CH_ATTACH_CLIENT | CH_ATTACH_MONITOR)

// struct monitor_channel;
typedef struct monitor_channel monitor_channel;
typedef struct MonitorChannelConfig MonitorChannelConfig;

/*
 * тк сейчас используем чисто shm_mq, но в будущем могут быть добавлены
 * и другие реализации, то имеет смысл сейчас (даже на всякий случай)
 * создать интерфейс для канала и добавлять реализации
 * по мере нужды
 *
 */

typedef enum
{
	Publisher,
	Subscriber
} ChannelRole;

/* 
 * QUESTION:
 * does it make any sense? 
 * 
 * TODO:
 * think about checking the status of the channel recipient 
 * separately and the status of the channel itself separately
 */
typedef enum
{
    CH_OK,
    
    /* Receive error codes */
    CH_RECV_EMPTY,
    CH_RECV_CLOSED,
    
    /* Send error codes */
    CH_SEND_WOULD_BLOCK,
    CH_SEND_DETACHED,
    // CH_SEND_NOT_READY,

    /* Common error codes */
    CH_UNEXPECTED_ERROR,
    CH_INVALID_ARG,
} ChannelOpResult;

typedef enum
{
    CH_UNUSED = 0,        
    CH_CREATED,           /* channel created by a client*/
	CH_ACTIVE,             /* channel is ready to use */
    CH_CLOSED
} ChannelState;

typedef enum
{
    CH_ATTACH_NONE      = 0,
    CH_ATTACH_CLIENT    = 1 << 0,
    CH_ATTACH_MONITOR  = 1 << 1
} ChannelAttachFlags;


typedef struct ChannelOps
{
    /*
     * init - Initialization of the certain type of channel
     * return true on success, else false 
     * 
     */
	bool (*init)(monitor_channel *ch, MonitorChannelConfig *arg);
	ChannelOpResult (*send_msg)(monitor_channel *ch, const void *data, Size len, bool nowait);
	ChannelOpResult (*receive_one_msg)(monitor_channel *ch, void *buf, Size buf_size, Size *out_len);
	void (*cleanup)(monitor_channel *ch);
	ChannelOpResult (*attach)(monitor_channel *ch);
    void (*detach)(monitor_channel *ch, void *local);

} ChannelOps;

typedef struct monitor_channel 
{
	const ChannelOps *ops;
	/* private implementation data (mb needed) */
    void *private_data;
	/* temporary */
	bool is_there_msgs; 
	int publisher_procno;
    int subscriber_procno;

	ChannelState state;
    uint8 attach_flags;
	
	slock_t mutex;

} monitor_channel;

static inline bool
channel_is_ready(uint8 flags)
{
    return (flags & (CH_ATTACH_CLIENT | CH_ATTACH_MONITOR))
           == (CH_ATTACH_CLIENT | CH_ATTACH_MONITOR);
};

#endif /* MONITOR_CHANNEL */
