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
#include "utils/timestamp.h"

#define BIT_WORD(idx) ((idx) / 64)
#define BIT_MASK(idx) (1ULL << ((idx) % 64))

#define LOG_LEVEL LOG
#define SHOULD_CREATE_NEW_ENTRY_IN_HASH_SUB true

// static int mss_alloc_subject_id(void);

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
     */ 
    /* find a place for subscriber */
    int sub_id = -1;
    int monitor_proc_no;
	SubscriberInfo *mySubInfo;
    monitor_channel *myChannel;
    MssState_SubscriberInfo *sharedSubInfo = &monSubSysLocal.MonSubSystem_SharedState->sub;
    bool is_channel_created;
    ChannelOpResult attach_res;

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

    // LWLockAcquire(&sharedSubInfo->lock, LW_EXCLUSIVE);
    
    if (sharedSubInfo->current_subs_num  == sharedSubInfo->max_subs_num)
    {
        // LWLockRelease(&sharedSubInfo->lock);
        elog(LOG_LEVEL, "Maximum of supported subscribers is reached, a place for new pub couldn't be allocated");
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
        // LWLockRelease(&sharedSubInfo->lock);
        return -1;
    }

    /* allocate memory for the monitor channel */
    myChannel = &monSubSysLocal.MonSubSystem_SharedState->channels[sub_id + MAX_PUBS_NUM];

    conConfig->channel_id = sub_id + MAX_PUBS_NUM;
    conConfig->subscriber_procno = MyProcNumber;
    conConfig->publisher_procno = monitor_proc_no;
    elog(LOG_LEVEL, "\npg_monitor_con_connect.c line: %d\n  publisher_procno %d\nsubscriber_procno %d\npub_id %d\nmonitor_proc_no %d\n", __LINE__, conConfig->publisher_procno, conConfig->subscriber_procno, sub_id, monitor_proc_no);

    elog(LOG_LEVEL, "\npg_monitor_con_connect.c line: %d\n  conConfig->channel_id %d", __LINE__, conConfig->channel_id);
    
    is_channel_created = monitor_channel_options[conConfig->type]->init(myChannel, conConfig);
	elog(LOG_LEVEL, "\npg_monitor_con_connect.c line: %d\n  is_channel_created %d", __LINE__, is_channel_created);

    if (! is_channel_created) {
        // LWLockRelease(&sharedSubInfo->lock);
        elog(LOG_LEVEL, "Couldn't create a channel");
        return -1 ;
    }

    attach_res = monitor_channel_options[conConfig->type]->attach(myChannel);
    elog(LOG_LEVEL, "\npg_monitor_sub_connect.c line: %d\n  ATTACH_RES_RESULT %d", __LINE__, attach_res);
    
    if (attach_res != CH_OK)
    {
        // LWLockRelease(&sharedSubInfo->lock);
        /*
         * TODO:
         * clear memory in case of channel can't be attached...
         * or mind a problem
         */
        return -1 ;
    }
    elog(LOG_LEVEL, "\npg_monitor_sub_connect.c line: %d\n", __LINE__);


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
    // LWLockRelease(&sharedSubInfo->lock);
    
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
    ChannelOpResult attach_res;

    MonitorEnsureContext();
    
    LWLockAcquire(&monSubSysLocal.MonSubSystem_SharedState->lock, LW_EXCLUSIVE);
	monitor_proc_no = monSubSysLocal.MonSubSystem_SharedState->pgprocno;
	/*
	 * TODO:
	 * Check if monitor_proc_no is valid
	 */
    
    LWLockRelease(&monSubSysLocal.MonSubSystem_SharedState->lock);
    
    LWLockAcquire(&sharedPubInfo->lock, LW_EXCLUSIVE);
    
    if (sharedPubInfo->max_pubs_num  == sharedPubInfo->current_pubs_num)
    {
        LWLockRelease(&sharedPubInfo->lock);
        elog(LOG_LEVEL, "Maximum of supported publishers is reached, a place for new pub couldn't be allocated");
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
    elog(LOG_LEVEL, "\npg_monitor_pub_connect.c line: %d\n  publisher_procno %d\nsubscriber_procno %d\npub_id %d\nmonitor_proc_no %d\n", __LINE__, conConfig->publisher_procno, conConfig->subscriber_procno, pub_id, monitor_proc_no);
    
    is_channel_created = monitor_channel_options[conConfig->type]->init(myChannel, conConfig);
    elog(LOG_LEVEL, "\npg_monitor_pub_connect.c line: %d\n  is_channel_created %d", __LINE__, is_channel_created);

    if (! is_channel_created) {
        LWLockRelease(&sharedPubInfo->lock);
        elog(LOG_LEVEL, "Couldn't create a channel");
        return -1 ;
    }

    attach_res = monitor_channel_options[conConfig->type]->attach(myChannel);
    elog(LOG_LEVEL, "\npg_monitor_pub_connect.c line: %d\n  ATTACH_RES_RESULT %d", __LINE__, attach_res);
    
    if (attach_res != CH_OK)
    {
        LWLockRelease(&sharedPubInfo->lock);
        /*
         * TODO:
         * clear memory in case of channel can't be attached...
         * or mind a problem
         */
        return -1 ;
    }
    elog(LOG_LEVEL, "\npg_monitor_pub_connect.c line: %d\n", __LINE__);

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

void
pg_monitor_con_disconnect(void)
{
    MonSubSystem_LocalState *local = &monSubSysLocal;
    SubscriberInfo *sub = local->mySubInfo;
    monitor_channel *ch;
    MssState_SubscriberInfo *sharedSubInfo = &local->MonSubSystem_SharedState->sub;
    int sub_id;

    if (local->mySubInfo == NULL)
        return;

    sub_id = sub - sharedSubInfo->subscribers;
    Assert(sub_id >= 0 && sub_id <  MAX_SUBS_NUM);

    LWLockAcquire(&sub->lock, LW_EXCLUSIVE);

    ch = sub->channel;

    if (ch != NULL)
    {
        if (ch->ops && ch->ops->detach)
            ch->ops->detach(ch, local->subLocalData);

        if (ch->ops && ch->ops->cleanup)
            ch->ops->cleanup(ch);

        sub->channel = NULL;
    }
    monitor_remove_subscriber_from_all_subjects(sub_id);

    /* Cleanup subscriptions bitmap  */
    memset(sub->bitmap, 0, sizeof(sub->bitmap));

    sub->id = -1;
    sub->proc_pid = 0;

    LWLockRelease(&sub->lock);

    LWLockAcquire(&sharedSubInfo->lock, LW_EXCLUSIVE);
    if (sharedSubInfo->current_subs_num > 0)
        sharedSubInfo->current_subs_num--;
    LWLockRelease(&sharedSubInfo->lock);

    local->mySubInfo = NULL;
}

void
pg_monitor_pub_disconnect(void)
{
    MonSubSystem_LocalState *local = &monSubSysLocal;
    PublisherInfo *pub;
    monitor_channel *ch;
    MssState_PublisherInfo *sharedPubInfo;

    if (local->myPubInfo == NULL)
        return;

    pub = local->myPubInfo;
    sharedPubInfo = &local->MonSubSystem_SharedState->pub;

    SpinLockAcquire(&pub->mutex);

    ch = pub->channel;
    SpinLockRelease(&pub->mutex);

    if (ch != NULL)
    {
        if (ch->ops && ch->ops->detach)
            ch->ops->detach(ch, local->pubLocalData);

        if (ch->ops && ch->ops->cleanup)
            ch->ops->cleanup(ch);

        SpinLockAcquire(&pub->mutex);

        pub->channel = NULL;
        
        SpinLockRelease(&pub->mutex);
    }

    SpinLockAcquire(&pub->mutex);

    pub->id = -1;
    pub->proc_pid = 0;

    SpinLockRelease(&pub->mutex);

    LWLockAcquire(&sharedPubInfo->lock, LW_EXCLUSIVE);
    if (sharedPubInfo->current_pubs_num > 0)
        sharedPubInfo->current_pubs_num--;
    LWLockRelease(&sharedPubInfo->lock);

    local->myPubInfo = NULL;
}


/*
 * -1 means mistake
 * It set conConfig.channel_id
 * 
 * TODO:
 * think about adding SHOULD_CREATE_NEW_ENTRY_IN_HASH_SUB to args
 */
MonitorResult pg_monitor_subscribe_to_event(const char *event_string, routing_type _routing_type)
{
    MonSubSystem_LocalState *local = &monSubSysLocal;
    MssState_SubjectEntitiesInfo *entitiesInfo;
    mssSharedState *shared;
    SubscriberInfo *sub;
    SubjectEntity *subject;
    routing_type subj_rt;
    mssEntry *entry;
    SubjectKey key;
    int subjectId;
    int subId;
    int word;
    uint64 mask;
    int subj_word;
    uint64 subj_mask;

    if (local->mySubInfo == NULL)
    {
        elog(LOG_LEVEL, "Subscriber not registered");        
        return MSS_ERR_NOT_REGISTERED;
    }

    if (event_string == NULL)
    {
        elog(LOG_LEVEL, "Invalid arg: string is NULL");  
        return MSS_ERR_INVALID_ARG;
    }
        

    if (strlen(event_string) >= MAX_SUBJECT_LEN)
    {
        elog(LOG_LEVEL, "Invalid arg: string is too long: %ld", strlen(event_string));  
        return MSS_ERR_INVALID_ARG;
    }

    sub = local->mySubInfo;
    shared = local->MonSubSystem_SharedState;
    entitiesInfo = &shared->entitiesInfo;


    memset(&key, 0, sizeof(key));
    strlcpy(key.name, event_string, MAX_SUBJECT_LEN);

    entry = find_or_create_subject_entry(key.name, SHOULD_CREATE_NEW_ENTRY_IN_HASH_SUB);

    if (entry == NULL && SHOULD_CREATE_NEW_ENTRY_IN_HASH_SUB == true) {
        return MSS_ERR_NO_SUBJECTS_SLOTS_AVAILABLE;
    } else if (entry == NULL && SHOULD_CREATE_NEW_ENTRY_IN_HASH_SUB == false) {
        return MSS_ERR_NOT_REGISTERED;
    }
    
    /* entry != NULL */
    elog(LOG_LEVEL, "\npg_monitor_subscribe_to_event line %d\nEntry with the key %s already exists\n", __LINE__, key.name);
    subjectId = entry->subjectEntityId;
    subject = &entitiesInfo->subjectEntities[subjectId];

    SpinLockAcquire(&subject->mutex);
    subj_rt = subject->_routingType;
    SpinLockRelease(&subject->mutex);

    if (subj_rt != UNDEFINED && subject->_routingType != _routing_type)
    {
        elog(LOG_LEVEL, "Routing type mismatch"); 
        return MSS_ERR_ROUTING_MISMATCH;
    } else {
        SpinLockAcquire(&subject->mutex);
        subject->_routingType = _routing_type;
        SpinLockRelease(&subject->mutex);
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

    return MSS_OK;
}

MonitorResult pg_monitor_unsubscribe_from_event(const char *event_string)
{
    MonSubSystem_LocalState *local = &monSubSysLocal;
    MssState_SubjectEntitiesInfo *entitiesInfo;
    mssSharedState *shared;
    SubscriberInfo *sub;
    SubjectEntity *subject;
    mssEntry *entry;
    SubjectKey key;
    int subjectId;
    int subId;
    int word;
    uint64 mask;
    int subj_word;
    uint64 subj_mask;
    uint64 oldval;
    uint64 newval;

    if (local->mySubInfo == NULL)
    {
        elog(LOG_LEVEL, "Subscriber not registered");        
        return MSS_ERR_NOT_REGISTERED;
    }

    if (event_string == NULL)
    {
        elog(LOG_LEVEL, "Invalid arg: string is NULL");  
        return MSS_ERR_INVALID_ARG;
    }
        

    if (strlen(event_string) >= MAX_SUBJECT_LEN)
    {
        elog(LOG_LEVEL, "Invalid arg: string is too long: %ld", strlen(event_string));  
        return MSS_ERR_INVALID_ARG;
    }

    sub = local->mySubInfo;
    shared = local->MonSubSystem_SharedState;
    entitiesInfo = &shared->entitiesInfo;


    memset(&key, 0, sizeof(key));
    strlcpy(key.name, event_string, MAX_SUBJECT_LEN);

    entry = find_or_create_subject_entry(key.name, false);

    if (entry == NULL) {
        /*
         * TODO:
         * think about what error should be returned 
         * or just MSS_OK??
         */

        return MSS_OK;
    }

    /* entry != NULL */
    elog(LOG_LEVEL, "\npg_monitor_unsubscribe_to_event line %d\nEntry with the key %s already exists\n", __LINE__, key.name);
    subjectId = entry->subjectEntityId;
    subject = &entitiesInfo->subjectEntities[subjectId];

    subId = sub->id;

    word = BIT_WORD(subId);
    mask = BIT_MASK(subId);

    subj_word = BIT_WORD(subjectId);
    subj_mask = BIT_MASK(subjectId);

    /*
     * Check if subscribed
     */

    LWLockAcquire(&sub->lock, LW_SHARED);

    if ((sub->bitmap[subj_word] & subj_mask) == 0)
    {
        LWLockRelease(&sub->lock);
        /*
         * TODO:
         * think about what error should be returned 
         * or just MSS_OK??
         */
        return MSS_OK;
    }

    LWLockRelease(&sub->lock);

    /*
     * Update SubjectEntity bitmap_subs
     */

    do
    {
        oldval = pg_atomic_read_u64(&subject->bitmap_subs[word]);
        newval = oldval & ~mask;
    }
    while (!pg_atomic_compare_exchange_u64(&subject->bitmap_subs[word],
                                           &oldval,
                                           newval));

    /*
     * Update SubscriberInfo bitmap
     */

    LWLockAcquire(&sub->lock, LW_EXCLUSIVE);

    sub->bitmap[subj_word] &= ~subj_mask;

    LWLockRelease(&sub->lock);

    return MSS_OK;
}


// /* 
//  * Helper func for pg_monitor_subscribe_to_event()
//  * 
//  * MUST be called under local->MonSubSystem_SharedState->lock
//  */
// static int
// mss_alloc_subject_id(void)
// {
//     MssState_SubjectEntitiesInfo *entitiesInfo = &monSubSysLocal.MonSubSystem_SharedState->entitiesInfo;
//     for (int i = entitiesInfo->next_subject_hint; i < MAX_SUBJECT_NUM; i++)
//     {
//         int word = BIT_WORD(i);
//         uint64 mask = BIT_MASK(i);
//         uint64 old =
//         pg_atomic_fetch_or_u64(&entitiesInfo->subject_used[word], mask);
//         if ((old & mask) == 0)
//         {
//             entitiesInfo->next_subject_hint++;
//             return i;
//         }
//     }
//     return -1;
// }

// в случае, если не удалось уведомить о событии, быстро возврщает управление
/*
 * 
 * 
 */
MonitorResult pg_monitor_notify(const char *event_name, const void *data, bool reliable)
{
    monitor_channel *ch;
    ChannelOpResult send_result;
    bool nowait = !reliable;
    MonitorMsg msg;
    Size name_len = strlen(event_name);
    Size data_len = strlen(data);
    
    elog(LOG_LEVEL, "\npg_monitor_notify\nevent_name = %s\nevent_len = %ld\ndata=%s\ndata_len=%ld\n", event_name, name_len, (char *)data, data_len);

    if (name_len > MAX_SUBJECT_LEN || data_len > MAX_MONITOR_MESSAGE_LEN)
    {
        return MSS_ERR_INVALID_ARG;
    }

    memset(&msg, 0, sizeof(MonitorMsg));
    memcpy(msg.key.name, event_name, name_len);
    msg.ts = GetCurrentTimestamp();
    memcpy(msg.data, data, data_len);

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
        return MSS_CHANNEL_WRONG_STATE;
    }

    send_result = ch->ops->send_msg(ch,
                           (void*) &msg,
                           sizeof(msg),
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

MonitorResult
pg_monitor_receive(MonitorMsg *out_msg)
{
    monitor_channel *ch;
    ChannelOpResult recv_result;
    Size out_len = 0;

    if (out_msg == NULL)
        return MSS_ERR_INVALID_ARG;

    if (monSubSysLocal.mySubInfo == NULL)
        return MSS_ERR_NOT_REGISTERED;

    ch = monSubSysLocal.mySubInfo->channel;

    if (ch == NULL)
        return MSS_NO_CHANNEL;

    SpinLockAcquire(&ch->mutex);
    if (ch->state != CH_ACTIVE)
    {
        SpinLockRelease(&ch->mutex);
        return MSS_CHANNEL_WRONG_STATE;
    }
    SpinLockRelease(&ch->mutex);
        

    recv_result = ch->ops->receive_one_msg(ch,
                                           out_msg,
                                           sizeof(MonitorMsg),
                                           &out_len);

    switch (recv_result)
    {
        case CH_OK:

            /* sanity check */
            if (out_len != sizeof(MonitorMsg))
            {
                elog(WARNING, "pg_monitor_receive: unexpected message size: %ld",
                     out_len);
                return CH_UNEXPECTED_ERROR;
            }

            return MSS_OK;

        case CH_RECV_CLOSED:
            return MSS_DETACHED;


        case CH_RECV_EMPTY:
            return MSS_NO_MSGS;
        case CH_INVALID_ARG:
            return MSS_ERR_INVALID_ARG;
        case CH_UNEXPECTED_ERROR:
        default:
            elog(LOG_LEVEL,
                 "pg_monitor_receive: unexpected receive result");
            return MSS_UNEXPECTED_ERROR;
    }
}


/*
 * Helper function for pg_monitor_con_disconnect()
 * 
 * Is must be used under &sub->lock
 */
static void
monitor_remove_subscriber_from_all_subjects(int sub_id)
{
    MssState_SubjectEntitiesInfo *subjInfo = &monSubSysLocal.MonSubSystem_SharedState->entitiesInfo;
    SubscriberInfo *sub = &monSubSysLocal.MonSubSystem_SharedState->sub.subscribers[sub_id];

    int word = sub_id / 64;
    uint64 mask = ((uint64)1 << (sub_id % 64));

    /* iterate through all subjects*/
    for (int i = 0; i < MAX_SUBJECT_NUM; i++)
    {
        int subj_word = BIT_WORD(i);
        uint64 subj_mask = BIT_MASK(i);
        SubjectEntity *subject = &subjInfo->subjectEntities[i];
        uint64 oldval;
        uint64 newval;

        /*
        * Check if subscribed
        */

        // LWLockAcquire(&sub->lock, LW_EXCLUSIVE);

        if ((sub->bitmap[subj_word] & subj_mask) == 0)
        {
            // LWLockRelease(&sub->lock);
            /*
                * TODO:
                * think about what error should be returned 
                * or just MSS_OK??
                */
            continue;
        }

        /* Subscribed! */

        do
        {
            oldval = pg_atomic_read_u64(&subject->bitmap_subs[word]);
            newval = oldval & ~mask;
        }
        while (!pg_atomic_compare_exchange_u64(&subject->bitmap_subs[word],
                                               &oldval,
                                               newval));

        /*
         * Update SubscriberInfo bitmap
         * 
         * TODO:
         * think about whether it needed at all
         */

        sub->bitmap[subj_word] &= ~subj_mask;

        // LWLockRelease(&sub->lock);
    }
}