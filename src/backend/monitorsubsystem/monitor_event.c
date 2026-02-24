/*-------------------------------------------------------------------------
 *
 * monitor_event.c
 *	  API for using the Monitoring Subsystem
 *
 * IDENTIFICATION
 *	  src/backend/monitorsubsystem/monitor_event.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <string.h>
 
#include "postmaster/monitor.h"
#include "monitorsubsystem/monitor_channel_type.h"
#include "monitorsubsystem/monitor_channel.h"
#include "monitorsubsystem/monitor_event.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "utils/memutils.h"

#define BIT_WORD(idx) ((idx) / 64)
#define BIT_MASK(idx) (1ULL << ((idx) % 64))

#define LOG_LEVEL LOG

static int mss_alloc_subject_id(void);

/*
 * I don't know whether it's good idea to create 
 * monitorsubsystem/monitor_channel_type.c
 * ONLY for this definition, so currently I put it here
 * 
 */
const ChannelOps *monitor_channel_options[] = {
    [MONITOR_CHANNEL_SHM_MQ] = &ShmMqChannelOps,
};

static void
MonitorEnsureContext(void)
{
    if (monSubSysLocal.ctx == NULL)
    {
        monSubSysLocal.ctx =
            AllocSetContextCreate(TopMemoryContext,
                                  "MonitorSubsystemContext",
                                  ALLOCSET_DEFAULT_SIZES);
    }
}


/*
 * -1 means mistake
 * It set conConfig.channel_id
 * 
 * TODO: 
 * Think about creating enum or errors and description to them
 */
int pg_monitor_con_connect(MonitorChannelConfig *conConfig)
{
    /*
     * тут мы передаем конфиш (в нем тип канала и нужные опции)
     * после этого
     * 1 ищем место в массиве подписчиков и там регаемся
     * 2 создаем сам канал
     * 3 ставим в массиве подписчиков указатель на канал
     * 
     * что про канал 
     * - он должен располагаться в разделяемой памяти
     * (и все, что к нему относится)
     * 
     * 
     * для того, чтобы создавать КАНАЛ чисто по конфигу (то есть по типу и параметрам)
     * и НЕ мучиться с switch-case, нужен массив структур
     * в этом массиве структур по типу канала будет выдаваться все, что надо для создания этого канала
     * 
     * 
     */ 
    /* find a place for subscriber */
    int sub_id = -1;
    int monitor_proc_no;
	SubscriberInfo *mySubInfo;
    monitor_channel *myChannel;
    MssState_SubscriberInfo *sharedSubInfo = &monSubSysLocal.MonSubSystem_SharedState->sub;
    bool is_channel_created;

    MonitorEnsureContext();
    MemoryContextSwitchTo(monSubSysLocal.ctx);

    LWLockAcquire(&monSubSysLocal.MonSubSystem_SharedState->lock, LW_EXCLUSIVE);
	monitor_proc_no = monSubSysLocal.MonSubSystem_SharedState->pgprocno;
	/*
	 * TODO:
	 * Check if monitor_proc_no is valid
	 */
	elog(LOG_LEVEL, "\npg_monitor_con_connect.c line: %d\n  monitor_proc_no %d", __LINE__, monitor_proc_no);
    
    LWLockRelease(&monSubSysLocal.MonSubSystem_SharedState->lock);

    LWLockAcquire(&sharedSubInfo->lock, LW_EXCLUSIVE);
    
    if (sharedSubInfo->current_subs_num  == sharedSubInfo->max_subs_num)
    {
        elog(DEBUG1, "Maximum of supported subscribers is reached, a place for new pub couldn't be allocated");
        return -1;
    }

    for (int i = 0; i < sharedSubInfo->max_subs_num; i++)
    {
        SubscriberInfo *sub = &sharedSubInfo->subscribers[i];
        bool res = LWLockConditionalAcquire(&sub->lock, LW_EXCLUSIVE);
        if (!res) {
            /* it's supposed that smbd working on it, so let's continue*/
            continue;
        }
        /*
         * TODO: select more appropriate criteria that this 
         * SubscriberInfo is free and add additional checks
         * 
         */
        if (sub->id == -1) {
            mySubInfo = sub;
            sub_id = i;
            mySubInfo->proc_pid = MyProcPid;
            mySubInfo->id = sub_id;
            
            LWLockRelease(&sub->lock);
            break;
        }
        LWLockRelease(&sub->lock);
    }
	elog(LOG_LEVEL, "\npg_monitor_con_connect.c line: %d\n  sub_id %d", __LINE__, sub_id);
    
    if (sub_id == -1)
    {
        LWLockRelease(&sharedSubInfo->lock);
        return -1;
    }

    /* allocate memory for the monitor channel */
    myChannel = &monSubSysLocal.MonSubSystem_SharedState->channels[sub_id + MAX_PUBS_NUM];

    conConfig->channel_id = sub_id + MAX_PUBS_NUM;
    elog(LOG_LEVEL, "\npg_monitor_con_connect.c line: %d\n  conConfig->channel_id %d", __LINE__, conConfig->channel_id);
    
    is_channel_created = monitor_channel_options[conConfig->type]->init(myChannel, conConfig);
	elog(LOG_LEVEL, "\npg_monitor_con_connect.c line: %d\n  is_channel_created %d", __LINE__, is_channel_created);

    if (! is_channel_created) {
        LWLockRelease(&sharedSubInfo->lock);
        elog(LOG_LEVEL, "Couldn't create a channel");
        return -1 ;
    }

    myChannel->subscriber_procno = MyProcNumber;
    myChannel->publisher_procno = monitor_proc_no;

    /*
     * maybe it'd be easier just not to release lock 
     * immideatly after finding mySubInfo
     *
     * 
     */
    LWLockAcquire(&mySubInfo->lock, LW_EXCLUSIVE);

    mySubInfo->channel = myChannel;
    
    sharedSubInfo->current_subs_num++;
    monSubSysLocal.mySubInfo = mySubInfo;


    LWLockRelease(&mySubInfo->lock);
    LWLockRelease(&sharedSubInfo->lock);
    
    return 0;
}


/*
 * -1 means mistake
 * It set conConfig.channel_id
 * 
 * This function should be called by any process(except by MonitorProcess)
 * 
 * TODO: 
 * Think about creating enum or errors and description to them
 */
int pg_monitor_pub_connect(MonitorChannelConfig *conConfig)
{
    /* find a place for publisher */
    int pub_id = -1;
    int monitor_proc_no;
	PublisherInfo *myPubInfo;
    monitor_channel *myChannel;
    MssState_PublisherInfo *sharedPubInfo = &monSubSysLocal.MonSubSystem_SharedState->pub;
    bool is_channel_created;

    MonitorEnsureContext();
    
    LWLockAcquire(&monSubSysLocal.MonSubSystem_SharedState->lock, LW_EXCLUSIVE);
	monitor_proc_no = monSubSysLocal.MonSubSystem_SharedState->pgprocno;
	/*
	 * TODO:
	 * Check if monitor_proc_no is valid
	 */
    elog(LOG_LEVEL, "\npg_monitor_pub_connect.c line: %d\n  monitor_proc_no %d", __LINE__, monitor_proc_no);
    
    LWLockRelease(&monSubSysLocal.MonSubSystem_SharedState->lock);
    
    LWLockAcquire(&sharedPubInfo->lock, LW_EXCLUSIVE);
    
    if (sharedPubInfo->max_pubs_num  == sharedPubInfo->current_pubs_num)
    {
        elog(DEBUG1, "Maximum of supported publishers is reached, a place for new pub couldn't be allocated");
        return -1;
    }

    for (int i = 0; i < sharedPubInfo->max_pubs_num; i++)
    {
        PublisherInfo *pub = &sharedPubInfo->publishers[i];
        /*
         * TODO: select more appropriate criteria that this 
         * PublisherInfo is free and add additional checks
         */

        SpinLockAcquire(&pub->mutex);
        if (pub->id == -1) {
            myPubInfo = pub;
            pub_id = i;
            myPubInfo->proc_pid = MyProcPid;
            myPubInfo->id = pub_id;
            
            SpinLockRelease(&pub->mutex);
            break;
        }
        SpinLockRelease(&pub->mutex);
    }
    elog(LOG_LEVEL, "\npg_monitor_pub_connect.c line: %d\n  pub_id %d", __LINE__, pub_id);
    
    if (pub_id == -1)
    {
        LWLockRelease(&sharedPubInfo->lock);

        return -1;
    }
    
    /*
     * TODO:
     * allocate memory for the monitor channel
     */
    myChannel = &monSubSysLocal.MonSubSystem_SharedState->channels[pub_id];

    conConfig->channel_id = pub_id;
    conConfig->publisher_procno = MyProcNumber;
    conConfig->subscriber_procno = monitor_proc_no;
    
    is_channel_created = monitor_channel_options[conConfig->type]->init(myChannel, conConfig);
    elog(LOG_LEVEL, "\npg_monitor_pub_connect.c line: %d\n  is_channel_created %d", __LINE__, is_channel_created);

    if (! is_channel_created) {
        LWLockRelease(&sharedPubInfo->lock);
        elog(DEBUG1, "Couldn't create a channel");
        return -1 ;
    }

    /*
     * maybe it'd be easier just not to release lock 
     * immideatly after finding myPubInfo
     */
    SpinLockAcquire(&myPubInfo->mutex);
    myPubInfo->channel = myChannel;
    
    sharedPubInfo->current_pubs_num++;
    monSubSysLocal.myPubInfo = myPubInfo;


    SpinLockRelease(&myPubInfo->mutex);
    LWLockRelease(&sharedPubInfo->lock);
    
    return 0;
}


MonitorResult pg_monitor_subscribe_to_event(const char *event_string, routing_type _routing_type)
{
    MonSubSystem_LocalState *local = &monSubSysLocal;
    MssState_SubjectEntitiesInfo *entitiesInfo;
    mssSharedState *shared;
    SubscriberInfo *sub;
    SubjectEntity *subject;
    mssEntry *entry;
    bool found;
    SubjectKey key;
    int subjectId;
    int subId;
    int word;
    uint64 mask;
    int subj_word;
    uint64 subj_mask;

    if (local->mySubInfo == NULL)
    {
        elog(DEBUG1, "Subscriber not registered");        
        return MSS_ERR_NOT_REGISTERED;
    }

    if (event_string == NULL)
    {
        elog(DEBUG1, "Invalid arg: string is NULL");  
        return MSS_ERR_INVALID_ARG;
    }
        

    if (strlen(event_string) >= MAX_SUBJECT_LEN)
    {
        elog(DEBUG1, "Invalid arg: string is too long: %ld", strlen(event_string));  
        return MSS_ERR_INVALID_ARG;
    }

    sub = local->mySubInfo;
    shared = local->MonSubSystem_SharedState;
    entitiesInfo = &shared->entitiesInfo;


    memset(&key, 0, sizeof(key));
    strlcpy(key.name, event_string, MAX_SUBJECT_LEN);

    LWLockAcquire(&shared->lock, LW_EXCLUSIVE);


    entry = hash_search(shared->mss_hash,
                    (void *) &key,
                    HASH_FIND,
                    &found);

    if (!found)
    {
        subjectId = mss_alloc_subject_id();
        if (subjectId == -1)
        {
            LWLockRelease(&shared->lock);
            elog(DEBUG1, "No free subject slots"); 
            return MSS_ERR_NO_SUBJECTS_SLOTS_AVAILABLE;
        }

        subject = &entitiesInfo->subjectEntities[subjectId];
        subject->_routingType = _routing_type;

        for (int w = 0; w < MAX_SUBS_BIT_NUM; w++)
            pg_atomic_write_u64(&subject->bitmap_subs[w], 0);

        entry = hash_search(shared->mss_hash,
                            (void *) &key,
                            HASH_ENTER,
                            &found);
        Assert(!found);

        entry->subjectEntityId = subjectId;
    }
    else
    {
        subjectId = entry->subjectEntityId;
        subject = &entitiesInfo->subjectEntities[subjectId];

        if (subject->_routingType != _routing_type)
        {
            LWLockRelease(&shared->lock);
            elog(DEBUG1, "Routing type mismatch"); 
            return MSS_ERR_ROUTING_MISMATCH;
        }
    }

    /* update SubjectEntity bitmap */

    subId = sub->id;
    word = BIT_WORD(subId);
    mask = BIT_MASK(subId);

    pg_atomic_fetch_or_u64(&subject->bitmap_subs[word], mask);

    /* update SubscriberInfo bitmap */

    LWLockAcquire(&sub->lock, LW_EXCLUSIVE);

    subj_word = BIT_WORD(subjectId);
    subj_mask = BIT_MASK(subjectId);

    sub->bitmap[subj_word] |= subj_mask;

    LWLockRelease(&sub->lock);
    LWLockRelease(&shared->lock);

    return MSS_OK;
}




/* 
 * Helper func for pg_monitor_subscribe_to_event()
 * 
 * MUST be called under local->MonSubSystem_SharedState->lock
 */
static int
mss_alloc_subject_id(void)
{
    MssState_SubjectEntitiesInfo *entitiesInfo = &monSubSysLocal.MonSubSystem_SharedState->entitiesInfo;

    for (int i = entitiesInfo->next_subject_hint; i < MAX_SUBJECT_NUM; i++)
    {
        int word = BIT_WORD(i);
        uint64 mask = BIT_MASK(i);

        uint64 old =
        pg_atomic_fetch_or_u64(&entitiesInfo->subject_used[word], mask);

        if ((old & mask) == 0)
        {
            entitiesInfo->next_subject_hint++;
            return i;
        }

    }
    return -1;
}


// в случае, если не удалось уведомить о событии, быстро возврщает управление
/*
 * 
 * 
 */
MonitorResult pg_monitor_notify(const char *event_string, bool reliable)
{
    monitor_channel *ch;
    ChannelOpResult send_result;
    bool nowait = !reliable;

    if (monSubSysLocal.myPubInfo == NULL)
    {
        elog(LOG_LEVEL, "Publisher not registered");
        return MSS_ERR_NOT_REGISTERED;
    }

    ch = monSubSysLocal.myPubInfo->channel;

    /* 
     * TODO:
     * figure out what the status of the channel is and what to return
     * 
     * Currently - MSS_CHANNEL_WRONG_STATE
     */
    if (ch == NULL || ch->state != CH_ACTIVE)
    {
        elog(LOG_LEVEL, "Publisher channel not active");
        return -1;
    }

    send_result = ch->ops->send_msg(ch,
                           event_string,
                           strlen(event_string) + 1,
                           nowait);

    switch (send_result)
    {
        case CH_OK:
            return MSS_OK;

        case CH_SEND_WOULD_BLOCK:
            /* it makes no sense if it's supposed to be reliable notify */
            Assert(!reliable);
            return MSS_CHANNEL_BUSY;

        case CH_SEND_DETACHED:
            return MSS_DETACHED;

        case CH_INVALID_ARG: 
            return MSS_ERR_INVALID_ARG;


        case CH_UNEXPECTED_ERROR:
        default:
            elog(LOG_LEVEL, "\n%dpg_monitor_reliable_notify: strange error code returned", __LINE__);
            return CH_UNEXPECTED_ERROR;
    }
}

