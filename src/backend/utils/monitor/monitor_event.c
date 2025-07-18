/*-------------------------------------------------------------------------
 *
 * monitor_event.c
 *	  Routines for inter-process monitoring events
 *
 * IDENTIFICATION
 *	  src/backend/utils/monitor/monitor_event.c
 *
 *-------------------------------------------------------------------------
 */

#include "monitor_event.h"
#include "utils/monitor_event_types.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "miscadmin.h"

Size monitor_entry_init_size(void);
Size monitor_entries_subref_size(void);
Size monitor_subscriptions_size(void);

EventToSubscriberSet *eventToSubscriberSet;

Size
monitor_entry_init_size(void) {
    Size sz;

    sz = MONITOR_EVENT_NUM_TYPES * sizeof(EventToSubscriberEntry);
	return MAXALIGN(sz);

}

Size
monitor_entries_subref_size(void) {
    Size sz;

    sz = MONITOR_EVENT_NUM_TYPES * MAX_SUBSCRIBERS_PER_EVENT * sizeof(MonitorSubscription_Ref);
	return MAXALIGN(sz);
}

Size
monitor_subscriptions_size(void) {
    Size sz;

    sz = MAX_SUBSCRIBERS * sizeof(MonitorSubscription);
	
    return MAXALIGN(sz);
}

Size
MonitorShmemSize(void) {
    Size sz;

    sz = MAXALIGN(sizeof(EventToSubscriberSet));
    /* memory for etsentries */
    sz = add_size(sz, monitor_entry_init_size());
    /* memory for subscribtion_refs for all event types */
    sz = add_size(sz, monitor_entries_subref_size());
    /* memory for subscribtions at the main structure */
    sz = add_size(sz, monitor_subscriptions_size());

    return sz;
}

/*
 * Считаем память под нашу структуру
 *      + под саму EventToSubscriberSet
 *      +- под все ее компоненты типа массивов 
 *      (имеем указатель на массив в EventToSubscriberSet 
 *      и выделяем память под структуру, на которую указывает указатель )
 *      место под подписки 
 *      место под массив с ссылками 
 * 
 * Выделяем память расчитанного размера и проставляем все указатели
 * 
 */

void 
MonitorEventSystemInit(void) {
    bool        found;
    Size        sz;

    sz = MonitorShmemSize();
    eventToSubscriberSet = (EventToSubscriberSet *)
        ShmemInitStruct("Shared Memory Monitor Event Subsystem", sz, &found);

    /* Actually, it would be better with checks like " if (isUnderPostmaster) ", etc. */
    if (!found) {
        char *p = (char *)(eventToSubscriberSet + 
            add_size(MAXALIGN(sizeof(EventToSubscriberSet)), monitor_entry_init_size()));
        sz = 0;

        eventToSubscriberSet->subscriptions = NULL;
        eventToSubscriberSet->etsentries = (EventToSubscriberEntry*)
            ((char*)eventToSubscriberSet + MAXALIGN(sizeof(EventToSubscriberSet)));
        eventToSubscriberSet->max_nsubscription = MAX_SUBSCRIBERS;


        /* Initialization of entries */          
        for (int i = 0; i < MONITOR_EVENT_NUM_TYPES; i++) {
            EventToSubscriberEntry entry = eventToSubscriberSet->etsentries[i];
            eventToSubscriberSet->etsentries[i].event = i;
            eventToSubscriberSet->etsentries[i].nsubscribtion = 0;
            eventToSubscriberSet->etsentries[i].max_nsubscriptions = MAX_SUBSCRIBERS_PER_EVENT;
            eventToSubscriberSet->etsentries[i].subscribtion_refs = (MonitorSubscription_Ref *) p;

            /* set all refs to subscriptions to NULL*/
            for (size_t i = 0; i < MAX_SUBSCRIBERS_PER_EVENT; i++) {
                entry.subscribtion_refs[i] = NULL;
            }            

            p = p + MAX_SUBSCRIBERS_PER_EVENT * sizeof(MonitorSubscription_Ref);
        }

        p = (char *)(eventToSubscriberSet + 
            add_size(MAXALIGN(sizeof(EventToSubscriberSet)), monitor_entry_init_size()));
        sz = MAXALIGN(sizeof(EventToSubscriberSet));
        /* memory for etsentries */
        sz = add_size(sz, monitor_entry_init_size());
        /* memory for subscribtion_refs for all event types */
        sz = add_size(sz, monitor_entries_subref_size());
        p = (char *)(eventToSubscriberSet + sz);

        eventToSubscriberSet->subscriptions = (MonitorSubscription *)p;
        /* maybe here need to initialize subscriptions... */

        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            eventToSubscriberSet->subscriptions[i].ref_count = 0;
            eventToSubscriberSet->subscriptions[i].subscriber.fd = -1;
            eventToSubscriberSet->subscriptions[i].subscriber.pid = 0;
        }
    }
}

/*
 * src/backend/storage/ipc/ipci.c
 * CreateSharedMemoryAndSemaphores
 *  CalculateShmemSize - сюда надо будет добавить размер выделяемой памяти
 *  CreateOrAttachShmemStructs - тут можно взять примеры всяких штук, 
 *  к которым доступаются через разделяемую память, и посмотреть, как доступаться к моей структуре
 */

/*
 * нужно будет еще разобраться с счетчиками nsubscription
 */
int SubscribeToMonitorEvent(MonitorEvent event, pgsocket fd) {
    // по хорошему бы проверка, норм сокет или нет
    // хотя это можно и оставить на совести вызывающего функцию =)

    // проверка на валидность pid

    // проверка что event - реально MonitorEvent

    MonitorSubscription_Ref subscriprion_ref = NULL;
    MonitorSubscription_Ref prev_ref = NULL;
    EventToSubscriberEntry *entry = NULL;
    MonitorSubscriber subscriber;
    subscriber.fd = fd;
    subscriber.pid = MyProcPid;

    /* addind to subscriptions array*/
    /* check, if the subscriber in array already */
    for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
        MonitorSubscription_Ref ref = &(eventToSubscriberSet->subscriptions[i]);
        LWLockAcquire(&(ref->lock), LW_EXCLUSIVE);
        if (prev_ref != NULL) {
            LWLockRelease(&(prev_ref->lock));
        }
        if (ref->subscriber.pid == subscriber.pid && ref->subscriber.fd == subscriber.fd) {
            subscriprion_ref = ref;

            LWLockRelease(&(ref->lock));
            break;
        }
        prev_ref = ref;
        /* if it's last iteration, free last lock*/
        if (i == MAX_SUBSCRIBERS - 1) {
            LWLockRelease(&(ref->lock));
        }
    }

    /*
     * if needed subscriber isn't in array already,
     * then we find free place and take it!
     */    
    if (subscriprion_ref == NULL) {
        prev_ref = NULL;
        if (eventToSubscriberSet->nsubscription == eventToSubscriberSet->max_nsubscription) {
            /* every subscriptin slot is busy */
            return 1;
        }

        /*
         * a b c
         * берем b
         * освобождаем a
         * работаем с b
         * берем с 
         * освобождаем b
         * ...
         */
        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            MonitorSubscription_Ref ref = &(eventToSubscriberSet->subscriptions[i]);
            LWLockAcquire(&(ref->lock), LW_EXCLUSIVE);
            if (prev_ref != NULL) {
                LWLockRelease(&(prev_ref->lock));
            }
            if (ref->ref_count == 0) {
                ref->subscriber.fd = fd;
                ref->subscriber.pid = MyProcPid;

                subscriprion_ref = ref;
                LWLockRelease(&(ref->lock));
                break;
            }

            prev_ref = ref;
            /* if it's last iteration, free last lock*/
            if (i == MAX_SUBSCRIBERS) {
                LWLockRelease(&(ref->lock));
            }
        }

    }


    /*
     * if the subscriber is already subscribed to the event, no need to change ref_count
     * else - chenge ref_count
     */
    /* adding ref to subscription in event entry subscriptions array */
    entry = &(eventToSubscriberSet->etsentries[event]);
    LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);
    entry->nsubscribtion++;
    for (int i = 0; i < entry->max_nsubscriptions; i++) {
        MonitorSubscription_Ref ref = entry->subscribtion_refs[i];
        if (ref != NULL) {
            /* 
             * if this ref is already ref to needed subscription,
             * it means the subscriber is already in subscription ref array
             */
            if (ref == subscriprion_ref) {
                LWLockRelease(&(entry->lock));
                return 0;
            }

            /*
             * if ref is ref to free subscription, 
             */
            if (ref->ref_count == 0) {
                ref = subscriprion_ref;
                ref->ref_count +=1;   
                LWLockRelease(&(entry->lock));
                return 0;
            }
        }

        /* if ref = NULL, it means place is free */
        if (ref == NULL) {
            ref = subscriprion_ref;
            ref->ref_count +=1; 
            LWLockRelease(&(entry->lock));
            return 0;
        }
    } 

    return 1;
}

void UnsubscribeFromAllMonitorEvents(pid_t pid, pgsocket fd) {
    for (int i = 0; i < MONITOR_EVENT_NUM_TYPES; i++) {
        UnsubscribeFromMonitorEvent((MonitorEvent)i, fd, pid);
    }
    return;
}

int UnsubscribeFromMonitorEvent(MonitorEvent event, pgsocket fd, pid_t pid) {
    /* 
     * проверка что event - реально MonitorEvent
     * здесь не нужна проверка, норм сокет или нет...
     */


    /*
     * ищем подписку в списке ссылок на подписки
     * уменьшаем счетчик в подписке, приравниваем подписку к NULL
     */
    EventToSubscriberEntry *entry = NULL;

    entry = &(eventToSubscriberSet->etsentries[event]);
    LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);
    for (int i = 0; i < entry->max_nsubscriptions; i++) {
        MonitorSubscription_Ref ref = entry->subscribtion_refs[i];
        if (ref != NULL &&  ref->subscriber.pid == pid && (fd == PGINVALID_SOCKET || ref->subscriber.fd == fd)) {
            ref->ref_count -= 1;
            ref = NULL;
            LWLockRelease(&(entry->lock));
            return 0;
        }
    } 

    return 1;
}
