/*-------------------------------------------------------------------------
 *
 * monitor_channel_shm_mq.c
 *	  Implementation of monitor channel api based on shm_mq
 *
 * IDENTIFICATION
 *	  src/backend/monitorsubsystem/monitor_channel_shm_mq.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/monitor.h"
#include "monitorsubsystem/monitor_channel.h"
#include "monitorsubsystem/monitor_channel_type.h"
#include "monitorsubsystem/monitor_channel_shm_mq.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/memutils.h"


#define LOG_LEVEL LOG

const ChannelOps ShmMqChannelOps = {
	.init = shm_mq_channel_init,
	.send_msg = shm_mq_channel_send_msg,
	.receive_one_msg = shm_mq_channel_receive_msg,
	.cleanup = shm_mq_channel_cleanup,
    .attach = shm_mq_channel_attach
};

bool
/*
 * Initialization of shm_mq_channel
 * Set Latch of another-side user of channel
 * return true on success, else false 
 * 
 * Есть shm_mq_handle - это backend-local структура для уже 
 * существующей shm_mq, через который конкретный процесс
 * будет с ней работать
 * 
 * Контекст памяти, активный в момент создания shm_mq_attach,
 * должен прожить как минимум столько же, сколько и сама shm_mq
 * 
 * есть штучка: для shm_mq нет функций типа reconnect / reset_queue / replace_sender
 * 
 * там могут (не факт, что возникнут, но могут) возникнуть проблемы с ожиданием
 * Latch'ей и состояниями очереди (в shm_mq кольцевой буфер и тд - если что-то случится)
 * с записью посреди записи, то все может быть нехорошо...
 * (надо рассмотреть эту ситуацию, я ее пока не рассматривала)
 * то есть внутреннее состояние shm_mq может быть не reset-safe
 * 
 *  
 * src/test/modules/test_shm_mq/setup.c
 * строка 147 - пример как создавать shm_mq с помощью
 * toc и shm_toc_insert в DSM
 * 
 * TODO: 
 * mind all checks
 * think about if the calling process can be neither a publisher 
 * nor a subscriber?
 */
shm_mq_channel_init(monitor_channel *ch, MonitorChannelConfig *cfg)
{
	shm_toc *toc = monSubSysLocal.MonSubSystem_SharedState->channels_toc;
	Size sz = cfg->u.shm_mq.mq_size + sizeof(ShmMqChannelData) + sizeof(ShmMqChannelLocal);
	ShmMqChannelData *data;
	void *mq_space;

    int otherProcNo;
    

    /* CHECKS */
    if (cfg->publisher_procno < 0 || cfg->publisher_procno >= ProcGlobal->allProcCount) 
    {
        elog(LOG_LEVEL, "\nshm_mq_channel_init \nINVALID publisher procno: %d", ch->publisher_procno);
        return CH_INVALID_ARG;
    }

    if (cfg->subscriber_procno < 0 || cfg->subscriber_procno >= ProcGlobal->allProcCount) 
    {
        elog(LOG_LEVEL, "\nshm_mq_channel_init \nINVALID subscriber procno: %d", ch->subscriber_procno);
        return CH_INVALID_ARG;
    }

    otherProcNo = cfg->publisher_procno == MyProcNumber ? cfg->subscriber_procno : cfg->publisher_procno;

	data = shm_toc_allocate(toc, sz);
	mq_space = (void *)((char *)data + sizeof(ShmMqChannelData) + sizeof(ShmMqChannelLocal));

	data->mq = shm_mq_create(mq_space, cfg->u.shm_mq.mq_size);
    
    shm_toc_insert(toc, cfg->channel_id, data);

    SpinLockAcquire(&ch->mutex);
	ch->private_data = data;
	ch->ops = &ShmMqChannelOps;
    ch->state = CH_CREATED;
    ch->publisher_procno = cfg->publisher_procno;
    ch->subscriber_procno = cfg->subscriber_procno;
    /* Should it be set out of spinlock?? */
    shm_mq_set_sender(data->mq, &ProcGlobal->allProcs[ch->publisher_procno]);
    shm_mq_set_receiver(data->mq, &ProcGlobal->allProcs[ch->subscriber_procno]);
    SpinLockRelease(&ch->mutex);

    SetLatch(&ProcGlobal->allProcs[otherProcNo].procLatch);
	return true;
}

/*
 * TODO:
 * think about MemoryContext for operations with channels
 */
ChannelOpResult
shm_mq_channel_attach(monitor_channel *ch)
{
    MemoryContext oldcontext;
    monitor_channel *shared_channels = monSubSysLocal.MonSubSystem_SharedState->channels;
    ShmMqChannelData *data = ch->private_data;
    ShmMqChannelLocal *local;

    if (monSubSysLocal.ctx == NULL)
    {
        monSubSysLocal.ctx =
            AllocSetContextCreate(TopMemoryContext,
                                  "MonitorSubsystemContext",
                                  ALLOCSET_DEFAULT_SIZES);
    }
    oldcontext = MemoryContextSwitchTo(monSubSysLocal.ctx);

    // ???
    /* CHECKS */
    if (ch->publisher_procno < 0 || ch->publisher_procno >= ProcGlobal->allProcCount) 
    {
        elog(LOG_LEVEL, "\nshm_mq_channel_attach \nINVALID publisher procno: %d", ch->publisher_procno);
        return CH_INVALID_ARG;
    } else {
        elog(LOG_LEVEL, "\nshm_mq_channel_attach \nCorrect publisher procno: %d", ch->publisher_procno);  
    }

    if (ch->subscriber_procno < 0 || ch->subscriber_procno >= ProcGlobal->allProcCount) 
    {
        elog(LOG_LEVEL, "\nshm_mq_channel_attach \nINVALID subscriber procno: %d", ch->subscriber_procno);
        return CH_INVALID_ARG;
    } else {
        elog(LOG_LEVEL, "\nshm_mq_channel_attach \nCorrect subscriber procno: %d", ch->subscriber_procno); 
    }

	/* Here shold be smth with MemoryContext */
    local = palloc0(sizeof(ShmMqChannelLocal));
    local->handle = shm_mq_attach(data->mq, NULL, NULL);

    SpinLockAcquire(&ch->mutex);
    if (AmMonitorSubsystemProcess()) 
    {
        int channel_id;
        elog(LOG_LEVEL, "\nshm_mq_channel_attach MONITOR PROCESS line: %d\n", __LINE__);
        Assert(ch >= &shared_channels[0] && ch <  &shared_channels[MAX_MONITOR_CHANNELS_NUM - 1]);
        
        channel_id = ch- shared_channels; 
        monSubSysLocal.monitorLocal.channelsLocalData[channel_id] = local;
        ch->attach_flags |= CH_ATTACH_MONITOR;
    } else {
        elog(LOG_LEVEL, "\nshm_mq_channel_attach CLIENT PROCESS line: %d\n", __LINE__);
        if (MyProcNumber == ch->subscriber_procno) {
            monSubSysLocal.subLocalData = local;
        }
        else {
            monSubSysLocal.pubLocalData = local;
        }
        ch->attach_flags |= CH_ATTACH_CLIENT;
    }

    if (channel_is_ready(ch->attach_flags))
    {
        elog(LOG_LEVEL, "\nshm_mq_channel_attach line: %d\n CHANNEL IS READY\n", __LINE__);
        ch->state = CH_ACTIVE;
    }
    SpinLockRelease(&ch->mutex);

    MemoryContextSwitchTo(oldcontext);
	return CH_OK;   
}

/*
 * TODO:
 * mind all checks
 */
ChannelOpResult
shm_mq_channel_send_msg(monitor_channel *ch, const void *data, Size len, bool nowait)
{
    ShmMqChannelLocal *local;
	shm_mq_result result;

    local = (ShmMqChannelLocal *)
        monSubSysLocal.pubLocalData;

    Assert(local && local->handle);

    

    result = shm_mq_send(local->handle,
                         len,
                         data,
                         nowait,
                         true); /* force_flush */

	switch (result)
	{
	case SHM_MQ_SUCCESS:
        SpinLockAcquire(&ch->mutex);
        ch->is_there_msgs = true;
        SpinLockRelease(&ch->mutex);
		return CH_OK;
		break;
	case SHM_MQ_DETACHED:
        return CH_SEND_DETACHED;
		break;
	case SHM_MQ_WOULD_BLOCK:
		return CH_SEND_WOULD_BLOCK;
		break;
	
	default:
		elog(ERROR, "unexpected shm_mq_send result");
    	return false;
		break;
	}
}


/*
 * TODO:
 * mind all checks
 */
ChannelOpResult
shm_mq_channel_receive_msg(monitor_channel *ch, void *buf, Size buf_size, Size *out_len)
{
    ShmMqChannelLocal *local;
    shm_mq_result result;
    Size len;
    void *data;

    if (AmMonitorSubsystemProcess())
    {
        int channel_id;
        monitor_channel *shared_channels = monSubSysLocal.MonSubSystem_SharedState->channels;
        
        elog(LOG_LEVEL, "\nshm_mq_channel_receive_msg MONITOR PROCESS line: %d\n", __LINE__);
        Assert(ch >= &shared_channels[0] && ch <  &shared_channels[MAX_MONITOR_CHANNELS_NUM - 1]);
        
        channel_id = ch- shared_channels; 
        local = monSubSysLocal.monitorLocal.channelsLocalData[channel_id];
    } else {
        /* Берём локальный handle */
        local = (ShmMqChannelLocal *)
        monSubSysLocal.subLocalData;
        Assert(local && local->handle);
    }

    result = shm_mq_receive(local->handle,
                            &len,
                            &data,
                            true);   /* nowait */

    if (result == SHM_MQ_WOULD_BLOCK)
        return CH_RECV_EMPTY;

    if (result == SHM_MQ_DETACHED)
        return CH_RECV_CLOSED;

    if (result != SHM_MQ_SUCCESS)
        elog(ERROR, "unexpected shm_mq_receive result");

    if (len > buf_size)
        elog(ERROR, "message too large for buffer");

    memcpy(buf, data, len);

    if (out_len)
        *out_len = len;

    return CH_OK;
}

void
shm_mq_channel_cleanup(monitor_channel *ch)
{
	// ShmMqChannelData *priv = (ShmMqChannelData *)ch->private_data;

	// if (priv->mq_handle)
	// 	shm_mq_detach(priv->mq_handle);

	// pfree(priv);
	// ch->private_data = NULL;
}
