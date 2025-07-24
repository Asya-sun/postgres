/*-------------------------------------------------------------------------
 *
 * monitor_event.h
 *	  Routines for interprocess monitoring events
 *
 * src/include/monitor_event.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONITOR_EVENT_H
#define MONITOR_EVENT_H

#include "postgres.h"

#include "storage/latch.h"
#include "storage/lwlock.h"
#include "nodes/pg_list.h"
#include "utils/datetime.h"
#include "utils/monitor_event_types.h"

#include <sys/un.h> 
#include <sys/socket.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* It's discussable*/
#define MAX_SUBSCRIBERS 5
#define MAX_SUBSCRIBERS_PER_EVENT MAX_SUBSCRIBERS

/*
 * This structure is needed to simplify procedure of subscription to many
 * events at once.
 * 
 * and this is under question
 */
typedef struct MonitorEventSet MonitorEventSet;

extern Size MonitorShmemSize(void);
extern void MonitorEventSystemInit(void);

/* all this is under question */
// extern MonitorEventSet *CreateMonitorEventSet(int event_num);
// extern void FreeMonitorEventSet(MonitorEventSet *set);
// extern int	AddMonitorEventToSet(MonitorEventSet *set, uint32 events, pgsocket fd);
// extern int SubscribeToMonitorEventSet(MonitorEventSet *set, pgsocket fd);

extern int SubscribeToMonitorEvent(MonitorEvent event, pgsocket fd, struct sockaddr_un address);
/* 
 * Maybe this one need to have pid in params 
 * In case it would cleared from random process 
 */
/* 
 * Params:
 * pid - in case another process would clear some processes dead subscriptions
 * fd - the socket used for getting messages
 * 
 * return value:
 * 0 is success
 * 1 means there isn't such subscriber  
 */
extern int UnsubscribeFromMonitorEvent(MonitorEvent event, pgsocket fd, pid_t pid);
/* 
 * отписаться от всех событий 1 процессом, или 1 процессом с 1 сокетом... щищ 
 * pid - in case another process would clear some processes dead subscriptions
 * fd - get the right fd, if you'd like to unsubscribe from all events with
 * the exact fd, or PGINVALID_SOCKET if you'd like to unsubscribe from all events
 */
extern void UnsubscribeFromAllMonitorEvents(pid_t pid, pgsocket fd);
void NotifyMonitorEvent(MonitorEvent event, const char* message, pgsocket sckt);

/*
 * monitor event - событие
 * subscribe - наверное, тут можно как с wait_event - подписываться на целые сеты событий 
 * (для этого и нужна битовая маска, чтобы можно было просто ее создать у себя, а фукнцию подписки вызывать 1 раз)
 * notify - если произошло событие, то процесс, в котором произошло это событие, уведомляет подписчиков
 * где то будет лежать "таблица подписчиков и событий" - структура, в которой есть соответствие событие-подписчики
 * 
 * уведомление будет проходить скорее всего так - произошло событие => notify => всем подписчикам данного события шлется в "передатчик" сообщение
 * 
 * передатчик
 * сокет
 * 
 * 
 * потенциальные проблемы 
 * 1 если подписчик "умер", что с ним делать?
 * тогда же будет сигнал sigpipe - нужно будет просто 
 * добавить в обработчик сигнала "чистку" таблицы подписчиков и событий
 * правда, останется работка различать сокеты для этой системы и не для этой системы, но с этим потом разберемся
 * 2 много подписчиков => notify - это бутылочное горлышко
 * то есть если эта система будет использоваться слишком большим количеством процессов,
 * то уведомление о событиях станет проблемой...
 * с другой стороны, предполагается, что система мониторинга событий будет использоваться
 * внутренними процессами и бекендом для клиентского мониторинга (а вряд ли таких процессов будет сильно много),
 * а потому на данный момент это вполне рациональное решение
 * 3 буфер сокета переполнен => данные теряются - но это уже ответственность процесса, как часто проверять событие
 * 
 * что будет приходить в датаграмме(уведомлении о событии?)
 * 
 */


/*
 * Подписка на событие
 * подписка на событие  = добавление в "таблицу подписчиков и событий" + создание "передатчика"
 * может, стоит добавить функционал добавления "передатчика" пользователем?
 * 
 * 1 если пользователь думает, что какие то события более приоритетные, то он может проверять их наличие чаще
 * при этом, он подписывается на бОльшее число событий
 * но рано или поздно он хочет обработать все события
 * тогда можно добавить возможность пользователю выбирать порт 
 * ИЛИ еще он может создать несколько сетов ивентов - на каждый сет автоматически будет создаваться по сокету,
 * и пользователю будет лишь возвращаться необходимый сокет, и пусть пользователь сам на нем ждет
 * А еще можно подписываться
 * 
 * Пользователь создает сокет и передает его в параметрах
 * 
 * Если пользователь будет все равно сам создавать сокет, то можно и не делать штуку с сетом событий
 * Можно подписываться как на сет, так и на одно событие (можно такой интерфейс добавить в целом)
 * Подписываться на сет имеет смысл сразу, если там в этом сете будет какая-то экстра информация об обработке событий, etc
 * 
 * в целом, если пока сет не нужен и пользователь будет передавать свой файловый дескриптор, то можно
 * пока что сет и не создавать
 * 
 * Окей, в целом, если что, эту функциональность добавить будет не сложно
 * (создание сокета), поэтому сейчас не паримся
 */

typedef struct MonitorSubscriber
{
    // тк udp-соединение, надо еще несколько полей по соединению
    pgsocket	fd;	
    struct sockaddr_un address;

    pid_t pid;
    /* Тут бы по хорошему иметь еще какую-то метаинформацию */
} MonitorSubscriber;

typedef struct MonitorSubscription
{
    LWLock lock;
    int ref_count;
    // сюда бы вместо is_free добавить счетчик ссылок (количество ивентов, на которые подписан данный пописчик)
    
    // should it be pointer or just structure?..
    MonitorSubscriber subscriber;
} MonitorSubscription;

typedef MonitorSubscription*  MonitorSubscription_Ref;

typedef struct EventToSubscriberEntry {
    MonitorEvent event;

    /* number of subscribers to this kind of event*/
    int nsubscribtion;
    int max_nsubscriptions;

    /* 
     * lock is needed when working with subscribtion_refs
     * maybe it would be better change later on another synchronization primitive (mechanism)
     */
    LWLock lock;

    MonitorSubscription_Ref *subscribtion_refs;
} EventToSubscriberEntry;

typedef struct EventToSubscriberSet {
    EventToSubscriberEntry *etsentries;

    int nsubscription;
    int max_nsubscription;

    MonitorSubscription *subscriptions;
} EventToSubscriberSet;


/* I'm not sure about extern, but curently it's okay*/
extern EventToSubscriberSet *eventToSubscriberSet;

typedef struct MonitorEventMessage {
    MonitorEvent event;
    TimestampTz event_time;
    pid_t sender_pid;
    char *data; // Теперь просто указатель, так как будем парсить JSON
} MonitorEventMessage;

int ParseMonitorJson(const char* json, MonitorEventMessage *msg);
void FreeMonitorEventMessage(MonitorEventMessage* msg);
MonitorEventMessage* CheckMonitorEvent(pgsocket fd, int millisec_timeout, bool *error_happened);

bool is_valid_monitor_event(MonitorEvent event);


/*
 * SubscriberToEvent
 * информация по подписчику не дублируется
 * 
 * EventToSubscriber
 * + подписчики на событие известны сразу, их не надо искать
 * - информация по подписчикам дублируется
 * 
 * узкие места:
 * много подписчиков
 * 
 * 
 * В целом, можно сделать EventToSubscriber с некоторыми модификациями
 * чтобы не хранить дублирующуюся информацию про подписчиков, можно
 * сделать какую нибудь большую структуру с EventToSubscriberEntry и subscribers
 * и в EventToSubscriberEntry хранить ссылки на подписчиков.
 * Тогда в каждой записи будут храниться ссылки и информация не будет дублироваться.
 * src/include/nodes/pg_list.h - тут структура list / список
 * 
 */

/*
 * Можно будет посмотреть в буфферном кеше списки
 */

/*
 * На завтра 
 * ? разобраться MonitorEventSet или просто MonitorEvent для подписки
 * (может, можно сделать 2 интерфейса?)
 * ? доп поля во всяких подписчик - подписка - ... (еще это в целом можно сделать по ходу)
 * 
 * придумать формат уведомления
 * сделать уведомление
 * 
 * интерфейс уведомления
 * + интерфейс проверки событий
 * 
 * 
 * Итак, я дошла до отправки) (вроде)
 * 
 * Доп поля для отправки
 * 
 * 1 нужно, чтобы у каждого процесса была штука типа "мой мониторинговый сокет"
 * 2 каждый процесс при подписке сам настраивает себе сокет
 * 
 * наверное, в момент подписки нужно создавать структурку типа
 * 
 * надо вообще сейчас все делать на unix domain socket, там нет портов и тд
 * они привязываются к файлу(но не всегда, только если самому указать его...)
 * 
 * тогда нужно сделать дополнение во время подписки: добавить 
 * структуру под сокеты в структуру подписчика, и инициализировать ее во время подписки
 * 
 * Итак, подписка
 * отписка
 * notify
 * 
 * 
 * теперь надо сделать проверку на событие - пришло ли событие???
 * скорее всего, это будет сделано с помощью epol/select ... 
 * 
 * 
 * общая логика примерно такая: 
 * челик проверяет, пришло ли что то на сокет
 * если да, то ...
 * 
 * Что возвращать в случае, если пришло сообщение?
 * Ну в общем то, если пришло сообщение, то парсим json => получаем 
 * номер события и пид, в котором это все произошло, и сообщение => 
 * Ну, вот и буду это возвращать) массив из MonitorEventMessage (если их пришло несколько...)
 * 
 * что лучше - epoll / select / etc?
 * корутины???
 * В общем-то, в данный момент я пишу все скорее под линукс, поэтому можно не париться
 * и делать как удобно
 */


#endif                          /* MONITOR_EVENT_H */
