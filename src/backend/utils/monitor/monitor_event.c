/*-------------------------------------------------------------------------
 *
 * monitor_event.c
 *	  Routines for inter-process latches
 *
 * IDENTIFICATION
 *	  src/backend/utils/monitor/monitor_event.c
 *
 *-------------------------------------------------------------------------
 */

#include "monitor_event.h"
#include "storage/shmem.h"



/*
 * Надо продумать, если эта штука будет лежать в разделяемой памяти, 
 * как к ней доступаться?
 * Для начала можно сделать кое кое, а потом доделать
 */
EventToSubscriberSet *eventToSubscriberSet;

Size
monitor_entry_init_size(void) {
    Size sz;

    sz = MONITOR_EVENT_NUM_TYPES * sizeof(EventToSubscriberEntry);
	return MAXALIGN(sz);

}

Size
monitor_entries_subref_size() {
    Size sz;

    sz = MONITOR_EVENT_NUM_TYPES * MAX_SUBSCRIBERS_PER_EVENT * sizeof(MonitorSubscription_Ref);
	return MAXALIGN(sz);
}

Size
monitor_subscriptions_size() {
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
InitializeMonitorEventSystem(void) {
    bool        found;
    Size        sz;

    sz = MonitorShmemSize();
    eventToSubscriberSet = (MonitorEventSet *)
        ShmemInitStruct("Shared Memory Monitor Event Subsystem", sz, &found);

    /* Actually, it would be better with checks like " if (isUnderPostmaster) ", etc. */
    if (!found) {
        Size sz;
        char *p = (char *)(eventToSubscriberSet + 
            add_size(MAXALIGN(sizeof(EventToSubscriberSet)), monitor_entry_init_size()));

        eventToSubscriberSet->subscriptions = NIL;
        eventToSubscriberSet->etsentries = (EventToSubscriberEntry*)
            ((char*)eventToSubscriberSet + MAXALIGN(sizeof(EventToSubscriberSet)));
        eventToSubscriberSet->max_nsubscription = MAX_SUBSCRIBERS;


        /* Initialization of entries */          
        for (int i = 0; i < MONITOR_EVENT_NUM_TYPES; i++) {
            eventToSubscriberSet->etsentries[i].event = i;
            eventToSubscriberSet->etsentries[i].nsubscribtion = 0;
            eventToSubscriberSet->etsentries[i].max_nsubscriptions = MAX_SUBSCRIBERS_PER_EVENT;
            eventToSubscriberSet->etsentries[i].subscribtion_refs = (MonitorSubscription_Ref *) p;

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
            // ...
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
