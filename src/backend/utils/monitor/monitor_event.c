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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <fcntl.h>

#include "mb/pg_wchar.h"

#include <netdb.h>
#include <errno.h>

#ifndef WIN32
#define sock_errno() errno
#define sock_strerror(err, buf, len) strerror_r(err, buf, len)
#else
#define sock_errno() WSAGetLastError()
#define sock_strerror(err, buf, len) strerror_s(buf, len, err)
#endif

#define MAX_MESSAGES_AT_TIME 16
#define SOCKET_PATH "/tmp/uds_epoll_example.sock"

EventToSubscriberSet *eventToSubscriberSet;

char* event_to_json(MonitorEvent event, const char* message, size_t *len);

char* event_to_json(MonitorEvent event, const char* message, size_t *len) {
    StringInfoData json;
    initStringInfo(&json);
    
    appendStringInfo(&json, "{");
    appendStringInfo(&json, "\"event\":%d,", event);
    appendStringInfo(&json, "\"time\":%lld,", (long long)GetCurrentTimestamp());
    appendStringInfo(&json, "\"pid\":%d,", MyProcPid);
    // escape_json(&json, message);  // Экранирование спецсимволов в строке
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

    /* Making buffer ready [1 bytes length] [json] */
    
    size_t total_size = sizeof(uint16_t) + len;
    char* buffer = palloc(total_size);
    
    // Записываем длину (сетевой порядок)
    uint16_t net_len = htons(len);
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
            ssize_t num = 0;
            socklen_t addr_len;
            struct sockaddr_un *addr;

            LWLockAcquire(&(sub->lock), LW_EXCLUSIVE);
            
            
            len = sizeof(struct sockaddr);            
            addr_len = sizeof(sa_family_t) + strlen(((struct sockaddr_un *)&sub->subscriber.address)->sun_path) + 1;

            addr = (struct sockaddr_un *)&sub->subscriber.address;
            addr_len = sizeof(sa_family_t) + strlen(addr->sun_path) + 1;

            elog(LOG, "Sending to socket: path='%s', addr_len=%d total_size=%ld", addr->sun_path, addr_len, total_size);
            if (access(addr->sun_path, F_OK) == -1) {
                elog(LOG, "Socket file %s does not exist!", addr->sun_path);
                continue;
            }

            // MSG_NOSIGNAL - Don't generate a SIGPIPE signal if the peer on a stream-oriented
            // socket has closed the connection.
            // Тут надо добавить проверку на ошибку (если это epipe, то видимо подписчик умер, и его надо убрать из таблицы)
            num = sendto(sckt, buffer, total_size, MSG_NOSIGNAL, 
                        (struct sockaddr *)addr, addr_len);

                        
            LWLockRelease(&(sub->lock));
            if (num == -1) {
                // Получаем текст ошибки
                char errbuf[256];
                int err = sock_errno();
                sock_strerror(err, errbuf, sizeof(errbuf));
                
                elog(LOG, "Failed to send event to subscriber %d: %s (errno=%d)", i, errbuf, err);

                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    fprintf(stderr, "Socket temporarily unavailable (would block)\n");
                } else {
                    perror("sendto");
                }
                
                // Под вопросиком...
                // // Если соединение разорвано, удаляем подписчика
                // if (err == EPIPE || err == ECONNRESET) {
                //     elog(LOG, "Removing dead subscriber %d", i);
                //     entry->subscribtion_refs[i] = NULL;
                //     entry->nsubscribtion--;
                //     sub->ref_count--;
                // }

            } else if (num != total_size) {
                elog(LOG, "DIDN'T SUCCED sending %ld bytes, sended %ld bytes", total_size, num);
            } else if (num == total_size) {
                elog(LOG, "succed sending %ld bytes, sended %ld bytes", total_size, num);
            }
        }
    }
    LWLockRelease(&(entry->lock));
    
    /* Clearing */
    pfree(buffer);
    pfree(json_string);
}






////////////////////////////
// Checking monitoring events on epoll

/*
 * Checking monitoring events
 * Params:
 * fd - fd to check monivents on
 * millisec_timeout - timeout to wait events for; -1 = infinity
 * надо еще как то отличать return NULL из-за ошибки и из -за отссутствия сообщений
 * 
 * if smth crusial happened, error_happened = true and watch logs
 */
MonitorEventMessage* CheckMonitorEvent(pgsocket fd, int millisec_timeout, bool *error_happened, int *mnum) {
    MonitorEventMessage *messages = palloc0(sizeof(MonitorEventMessage) * MAX_MESSAGES_AT_TIME);
    struct epoll_event ev, events[MAX_MESSAGES_AT_TIME];
    char buf[1024];
    int nfds, epollfd;
    int nmsgs = 0;

    *error_happened = false;

    /* Code to set up listening socket, 'listen_sock',
        (socket(), bind(), listen()) omitted. */

    epollfd = epoll_create1(0);
    if (epollfd == -1) {
        elog(WARNING, "[ PID = %d ] epoll_create1", MyProcPid);
        goto epoll_create1_error;
    }

    ev.events = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        elog(WARNING, "[ PID = %d ] epoll_ctl: fd", MyProcPid);
        goto epoll_ctl_error;
    }


    nfds = epoll_wait(epollfd, events, MAX_MESSAGES_AT_TIME, millisec_timeout);
    if (nfds == -1) {
        elog(WARNING, "[ PID = %d ] epoll_wait", MyProcPid);
        goto epoll_wait_error;
    }    

    for (int n = 0; n < nfds; ++n) {
        if (events[n].data.fd == fd) {
            int res = 0;
            ssize_t recv_len;
            memset(buf, 0, sizeof(buf));
            recv_len = recvfrom(fd, buf, sizeof(buf), 0, NULL, NULL);
            if (recv_len == -1) {
                // /* Тут на самом деле большой вопрос, что конкретно делать */
                // elog(WARNING, "[ PID = %d ] recvfrom: %m", MyProcPid);
                // continue;
                int saved_errno = errno;
                perror("sendto");

                /* Error that okay to ignore */
                if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK || saved_errno == EINTR) {
                    elog(WARNING,  "[ PID = %d ] recvfrom: %m", MyProcPid);  // Логируем для отладки
                    continue;
                }

                /* Crucial errors */
                elog(WARNING,  "[ PID = %d ] recvfrom failed: %m", MyProcPid);
                
                /* Crusial errors check */
                if (saved_errno == ENOMEM || saved_errno == EFAULT || saved_errno == EBADF) {

                    elog(WARNING,  "[ PID = %d ] CRUCIAL recvfrom failed: %m", MyProcPid);
                    *error_happened = true;
                    break;
                }
            }

            buf[recv_len] = '\0';
            elog(INFO, "Got message: %s", buf);
            res = ParseMonitorJson(buf, &(messages[nmsgs]));
            if (res != 0) {
                elog(WARNING, "not suceed parsing");
                continue;
            }
            nmsgs++;
        }
    }

    close(epollfd);
    *mnum = nmsgs;
    return messages;

epoll_ctl_error:
epoll_wait_error:
    close(epollfd);
epoll_create1_error:
    pfree(messages);
    *error_happened = true;
    return NULL;
}

void FreeMEMessagesAfterEvent(MonitorEventMessage *msg, int nmsg) {
    for (int i = 0; i < MAX_MESSAGES_AT_TIME; i++) {
        if (msg[i].data != NULL) {
            pfree(msg[i].data);
        }
    }
    if (msg != NULL) {
        pfree(msg);
    }
}

/* State for parser */
typedef struct {
    MonitorEventMessage *msg;
    char *current_key;
} ParseState;


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
    Size sz = 0;

    sz = sizeof(MonitorSubscription_Ref);
	sz *= (Size) MONITOR_EVENT_NUM_TYPES;
    sz *= (Size) MAX_SUBSCRIBERS_PER_EVENT;
    
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

void MonitorEventSystemInit(void) {
    bool found;
    Size sz;
    uintptr_t *ptr;

    sz = MonitorShmemSize();
    eventToSubscriberSet = (EventToSubscriberSet *)
        ShmemInitStruct("Shared Memory Monitor Event Subsystem", sz, &found);
    
    if (!IsUnderPostmaster) {
        Assert(!found);
        memset(eventToSubscriberSet, 0, sz);
        
        ptr = (uintptr_t *)eventToSubscriberSet;
        ptr += MAXALIGN(sizeof(EventToSubscriberSet));
        
        eventToSubscriberSet->max_nsubscription = MAX_SUBSCRIBERS;
        eventToSubscriberSet->etsentries = (EventToSubscriberEntry *)ptr;
        ptr += monitor_entry_init_size() ;
        

        for (int i = 0; i < MONITOR_EVENT_NUM_TYPES; i++) {
            EventToSubscriberEntry *entry = &eventToSubscriberSet->etsentries[i];
            entry->event = i;
            entry->nsubscribtion = 0;
            entry->max_nsubscriptions = MAX_SUBSCRIBERS_PER_EVENT;
            entry->subscribtion_refs = (MonitorSubscription_Ref *)ptr;
        
            ptr += MAXALIGN(MAX_SUBSCRIBERS_PER_EVENT * sizeof(MonitorSubscription_Ref));
            
            for (int j = 0; j < MAX_SUBSCRIBERS_PER_EVENT; j++) {
                entry->subscribtion_refs[j] = NULL;
            }
            
            LWLockInitialize(&entry->lock, LWTRANCHE_MONITOR_EVENT);
        }
        
        eventToSubscriberSet->subscriptions = (MonitorSubscription *)ptr;
    
        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            MonitorSubscription *sub = &eventToSubscriberSet->subscriptions[i];
            sub->ref_count = 0;
            sub->subscriber.fd = PGINVALID_SOCKET;
            sub->subscriber.pid = 0;
            memset(&sub->subscriber.address, 0, sizeof(struct sockaddr_un));
            LWLockInitialize(&sub->lock, LWTRANCHE_MONITOR_EVENT);
        }
        
        /* Финальная проверка */
        if ((ptr + monitor_subscriptions_size() - (uintptr_t *)eventToSubscriberSet) > sz) {
            elog(ERROR, "Memory overflow detected");
        }
    } else {
        Assert(found);
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
 * 
 * Pgsocket should be non-blocking uds socket
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
            elog(LOG, "ALL SUBSRCIBTIONS ARE BUSY %d", __LINE__);
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
                memcpy(&(ref->subscriber.address), &address, sizeof(address));

                eventToSubscriberSet->nsubscription += 1;
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
    //////////////////////////////////////////////

    /*
     * if the subscriber is already subscribed to the event, no need to change ref_count
     * else - chenge ref_count
     */
    /* adding ref to subscription in event entry subscriptions array */
    entry = &(eventToSubscriberSet->etsentries[event]);
    LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);

    

    for (int i = 0; i < entry->max_nsubscriptions; i++) {
        MonitorSubscription_Ref *ref_ptr = &(entry->subscribtion_refs[i]);
        if (*ref_ptr != NULL) {
            /* 
             * if this ref is already ref to needed subscription,
             * it means the subscriber is already in subscription ref array
             */
            if (*ref_ptr == subscriprion_ref) {
                elog(LOG, "[ %d] already subscribed to event %d", MyProcPid, event);
                LWLockRelease(&(entry->lock));
                return 0;
            }
            /*
             * if ref is ref to free subscription, 
             */
            // Это меня смущает...
            if ((*ref_ptr)->ref_count == 0) {
                *ref_ptr = subscriprion_ref;
                (*ref_ptr)->ref_count +=1;   
                entry->nsubscribtion++;
                LWLockRelease(&(entry->lock));
                return 0;
            }
        } 
        /* if ref = NULL, it means place is free */
        else 
        {

            *ref_ptr = subscriprion_ref;
            (*ref_ptr)->ref_count +=1; 
            entry->nsubscribtion++;
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
    EventToSubscriberEntry *entry = NULL;

    /* There is no need to check if fd is valid or not*/
    if(!is_valid_monitor_event(event)) {
        elog(ERROR, "event type is not valid: %d", event);
        return 1;
    }

    /*
     * ищем подписку в списке ссылок на подписки
     * уменьшаем счетчик в подписке, приравниваем подписку к NULL
     */
    entry = &(eventToSubscriberSet->etsentries[event]);
    LWLockAcquire(&(entry->lock), LW_EXCLUSIVE);
    for (int i = 0; i < entry->max_nsubscriptions; i++) {
        MonitorSubscription_Ref *ref_ptr = &(entry->subscribtion_refs[i]);
        if (*ref_ptr != NULL &&  (*ref_ptr)->subscriber.pid == pid && (fd == PGINVALID_SOCKET || (*ref_ptr)->subscriber.fd == fd)) {
            (*ref_ptr)->ref_count -= 1;
            *ref_ptr = NULL;
            LWLockRelease(&(entry->lock));
            return 0;
        }
    } 

    return 1;
}



static JsonParseErrorType parse_object_start(void *state) {
    return JSON_SUCCESS;
}

static JsonParseErrorType parse_array_start(void *state) {
    return JSON_SUCCESS;
}

static JsonParseErrorType parse_object_end(void *state) {
    ParseState *pstate = (ParseState *)state;
    if (pstate && pstate->current_key) {
        pfree(pstate->current_key);
        pstate->current_key = NULL;
    }
    return JSON_SUCCESS;
}

static JsonParseErrorType parse_array_end(void *state) {
    return JSON_SUCCESS;
}


static JsonParseErrorType object_field_start(void *state, char *fname, bool isnull) {
    ParseState *pstate = (ParseState *)state;
    
    if (!pstate) {
        elog(ERROR, "ParseState is NULL in object_field_start");
        return JSON_INVALID_TOKEN;
    }

    if (!fname || isnull) {
        elog(LOG, "Warning: NULL field name encountered");
        pstate->current_key = NULL;
        return JSON_EXPECTED_MORE;
    }

    pstate->current_key = pstrdup(fname);
    return JSON_SUCCESS;
}

static JsonParseErrorType parse_scalar(void *state, char *token, JsonTokenType tokentype) {
    ParseState *pstate = (ParseState *)state;
    
    if (!pstate) return JSON_INVALID_TOKEN;
    
    if (!pstate->current_key) {
        elog(LOG, "Skipping scalar with NULL key (token: %s)", token);
        return JSON_EXPECTED_MORE;
    }

    // elog(LOG, "Parsing scalar: %s = %s", pstate->current_key, token);
    
    if (strcmp(pstate->current_key, "event") == 0) {
        pstate->msg->event = atoi(token);
    }
    else if (strcmp(pstate->current_key, "time") == 0) {
        pstate->msg->event_time = atoll(token);
    }
    else if (strcmp(pstate->current_key, "pid") == 0) {
        pstate->msg->sender_pid = atoi(token);
    }
    else if (strcmp(pstate->current_key, "data") == 0) {
        // Убедимся, что не перезаписываем существующий указатель
        if (pstate->msg->data) {
            pfree(pstate->msg->data);
        }
        pstate->msg->data = pstrdup(token);
    }

    // pfree(pstate->current_key);
    pstate->current_key = NULL;
    return JSON_SUCCESS;
}



int ParseMonitorJson(const char* json, MonitorEventMessage *msg) {
    JsonLexContext *lex;
    JsonParseErrorType error;
    ParseState state;
    JsonSemAction sem;
    uint16_t net_len;
    uint16_t len;
    size_t real_size;
    char *json_str;

    // 1. Проверка входных данных
    if (!json || !msg) {
        elog(WARNING, "NULL pointer passed to ParseMonitorJson");
        return 1;
    }

    // 2. Извлечение длины сообщения

    net_len = *((uint16_t*)json);
    len = ntohs(net_len);
    json_str = (char*)(json + sizeof(uint16_t));

    // 3. Проверка длины
    real_size = strlen(json_str);
    if (len != real_size) {
        elog(WARNING, "Length mismatch: header=%d, actual=%zu", len, real_size);
        return 1;
    }

    // elog(LOG, "Parsing JSON: %.*s", (int)len, json_str);

    // 4. Инициализация состояния
    memset(&state, 0, sizeof(ParseState));
    state.msg = msg;

    // 5. Настройка парсера
    memset(&sem, 0, sizeof(JsonSemAction));
    sem.semstate = (void *)&state;
    sem.object_start = parse_object_start;
    sem.array_start = parse_array_start;
    sem.object_field_start = object_field_start;
    sem.scalar = parse_scalar;
    sem.object_end = parse_object_end;
    sem.array_end = parse_array_end;

    // 6. Создание контекста парсера

    lex = makeJsonLexContextCstringLen(NULL, json_str, len, PG_UTF8, true);

    // 7. Парсинг
    error = pg_parse_json(lex, &sem);
    if (error != JSON_SUCCESS) {
        char *errmsg = json_errdetail(error, lex);
        elog(ERROR, "JSON parse error: %s", errmsg);
        pfree(errmsg);
        freeJsonLexContext(lex);
        return 1;
    }

    // 8. Проверка результата
    if (!state.msg->data) {
        elog(WARNING, "No data field found in JSON");
    }

    freeJsonLexContext(lex);
    return 0;
}
