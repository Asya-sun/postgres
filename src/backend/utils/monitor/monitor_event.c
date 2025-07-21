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
#include "common/jsonapi.h"
#include "utils/json.h"
#include "utils/monitor_event_types.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "miscadmin.h"

#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "postgres.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/jsonfuncs.h"


EventToSubscriberSet *eventToSubscriberSet;

char* event_to_json(MonitorEvent event, const char* message, size_t *len);

char* event_to_json(MonitorEvent event, const char* message, size_t *len) {
    StringInfoData json;
    initStringInfo(&json);
    
    appendStringInfo(&json, "{");
    appendStringInfo(&json, "\"event\":%d,", event);
    appendStringInfo(&json, "\"time\":%lld,", (long long)GetCurrentTimestamp());
    appendStringInfo(&json, "\"pid\":%d,", MyProcPid);
    escape_json(&json, message);  // Экранирование спецсимволов в строке
    appendStringInfo(&json, "\"data\":\"%s\"", message);
    appendStringInfo(&json, "}");
    
    *len = json.len;
    return json.data;
}


void NotifyMonitorEvent(MonitorEvent event, const char* message, pgsocket sckt) {
    EventToSubscriberEntry *entry;
    /* Forming json */
    size_t len = 0;
    char* json_string = event_to_json(event, message, &len);

    /* Making buffer ready [4 bytes length] [json] */
    
    size_t total_size = sizeof(uint32_t) + len;
    char* buffer = palloc(total_size);
    
    // Записываем длину (сетевой порядок)
    uint32_t net_len = htonl(len);
    memcpy(buffer, &net_len, sizeof(net_len));
    
    // Копируем JSON
    memcpy(buffer + sizeof(net_len), json_string, len);

    /* 
     * ТУТ НАДО СДЕЛАТЬ ЧТО-ТО ДЛЯ ПОЛУЧЕНИЯ СОКЕТА С КОТОРОГО ОТПРАВЛЯТЬ
     * пока что это в параметрах, однако в будущем посмотрим...
     */
    
    /* Sending */
    entry = &(eventToSubscriberSet->etsentries[event]);
    LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);
    for (int i = 0; i < entry->max_nsubscriptions; i++) {
        MonitorSubscription_Ref sub = entry->subscribtion_refs[i];
        if (sub != NULL) {
            LWLockAcquire(&(sub->lock), LW_EXCLUSIVE);
            len = sizeof(struct sockaddr);
            // MSG_NOSIGNAL - Don't generate a SIGPIPE signal if the peer on a stream-oriented
            // socket has closed the connection.
            // Тут надо добавить проверку на ошибку (если это epipe, то видимо подписчик умер, и его надо убрать из таблицы)
            sendto(sckt, buffer, total_size, MSG_NOSIGNAL, (struct sockaddr *) &(sub->subscriber.address), (socklen_t) len);
            LWLockRelease(&(sub->lock));
        }
    }
    LWLockRelease(&(entry->lock));
    
    /* Clearing */
    pfree(buffer);
    pfree(json_string);
}



/* State for parser */
typedef struct {
    MonitorEventMessage *msg;
    char *current_key;
} ParseState;


/* Callback when the key is detected */
static JsonParseErrorType object_field_start(void *state, char *fname, bool isnull)
{
    ParseState *pstate = (ParseState *)state;
    pstate->current_key = pstrdup(fname);
    return JSON_SUCCESS;
}


/* Callback whan the value is detected*/
static JsonParseErrorType parse_scalar(void *state, char *token, JsonTokenType tokentype) {
    ParseState *pstate = (ParseState *)state;
    
    if (!pstate->current_key) return JSON_INVALID_TOKEN;
    
    if (strcmp(pstate->current_key, "event") == 0 && tokentype == JSON_TOKEN_NUMBER) {
        pstate->msg->event = atoi(token);
    }
    else if (strcmp(pstate->current_key, "time") == 0 && tokentype == JSON_TOKEN_NUMBER) {
        pstate->msg->event_time = atoll(token);
    }
    else if (strcmp(pstate->current_key, "pid") == 0 && tokentype == JSON_TOKEN_NUMBER) {
        pstate->msg->sender_pid = atoi(token);
    }
    else if (strcmp(pstate->current_key, "data") == 0 && tokentype == JSON_TOKEN_STRING) {
        pstate->msg->data = pstrdup(token);
    }
    
    pfree(pstate->current_key);
    pstate->current_key = NULL;
    return JSON_SUCCESS;
}


MonitorEventMessage* ParseMonitorJson(const char* json) {
    MonitorEventMessage* msg = palloc0(sizeof(MonitorEventMessage));
    JsonLexContext lex;
    text *result = cstring_to_text(json + sizeof(uint32_t));
    JsonParseErrorType error;
    ParseState state;
    JsonSemAction sem;
    
    /* Cheking if json is valid */
    if (!json) {
        ereport(ERROR,
                (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("json string cannot be NULL")));
        return NULL;
    }
    // Инициализация состояния
    state.msg = msg;
    state.current_key = NULL;
    
    // Настройка семантических действий
    memset(&sem, 0, sizeof(JsonSemAction));
    sem.semstate = (void *)&state;
    // sem.object_start = parse_object_start;
    sem.scalar = parse_scalar;
    sem.object_field_start = object_field_start;
    
    // Создание контекста лексера
    makeJsonLexContext(&lex, result, false);
    
    /* Parsing JSON */
    error = pg_parse_json(&lex, &sem);
    if (error != JSON_SUCCESS) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                 errmsg("invalid input syntax for json: %d", error)));
    }
    
    /* Free resources */
    freeJsonLexContext(&lex);
    if (state.current_key) {
        pfree(state.current_key);
    }
    pfree(result);
    
    return msg;
}

void FreeMonitorEventMessage(MonitorEventMessage* msg) {
    if (msg->data) pfree(msg->data);
    pfree(msg);
}


Size monitor_entry_init_size(void);
Size monitor_entries_subref_size(void);
Size monitor_subscriptions_size(void);



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
    sz = add_size(sz, monitor_entry_init_size());       /* memory for etsentries */
    sz = add_size(sz, monitor_entries_subref_size());       /* memory for subscribtion_refs for all event types */
    sz = add_size(sz, monitor_subscriptions_size());        /* memory for subscribtions at the main structure */

    return sz;
}


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

bool is_valid_monitor_event(MonitorEvent event) {
    return event >= MONITOR_EVENT_START && event < MONITOR_EVENT_NUM_TYPES;
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
int SubscribeToMonitorEvent(MonitorEvent event, pgsocket fd, struct sockaddr_un address) {
    // по хорошему бы проверка, норм сокет или нет
    // хотя это можно и оставить на совести вызывающего функцию =)

    // проверка на валидность pid

    MonitorSubscription_Ref subscriprion_ref = NULL;
    MonitorSubscription_Ref prev_ref = NULL;
    EventToSubscriberEntry *entry = NULL;
    MonitorSubscriber subscriber;
    subscriber.fd = fd;
    subscriber.pid = MyProcPid;

    if(!is_valid_monitor_event(event)) {
        elog(ERROR, "event type is not valid: %d", event);
        return 1;
    }

    

    /* addind to subscriptions array*/
    /* check, if the subscriber in array already */
    for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
        MonitorSubscription_Ref ref = &(eventToSubscriberSet->subscriptions[i]);
        LWLockAcquire(&(ref->lock), LW_EXCLUSIVE);
        if (prev_ref != NULL) {
            LWLockRelease(&(prev_ref->lock));
        }
        if (ref->subscriber.pid == subscriber.pid && 
                ref->subscriber.fd == subscriber.fd && 
                strcmp(ref->subscriber.address.sun_path, address.sun_path)) {
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
                ref->subscriber.address = address;

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
    /* There is no need to check if fd is valid or not*/
    if(!is_valid_monitor_event(event)) {
        elog(ERROR, "event type is not valid: %d", event);
        return 1;
    }


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
