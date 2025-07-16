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

#include "latch.h"
#include "nodes/pg_list.h"
#include "utils/monitor_event_types.h"

#define MAX_SUBSCRIBERS 64
#define MAX_SUBSCRIBERS_PER_EVENT 64

/*
 * This structure is needed to simplify procedure of subscription to many
 * events at once.
 * 
 * Если событие - бит, то MonitorEventSet - битовая маска
 * Если событие - enum / структура, то MonitorEventSet - как минимум массив
 */
typedef struct MonitorEventSet MonitorEventSet;


extern void InitializeMonitorEventSystem(void);

/* all this is under question */
extern MonitorEventSet *CreateMonitorEventSet(int event_num);
extern void FreeMonitorEventSet(MonitorEventSet *set);
extern int	AddMonitorEventToSet(MonitorEventSet *set, uint32 events, pgsocket fd);
extern int SubscribeToMonitorEventSet(MonitorEventSet *set, pgsocket fd);

extern int SubscribeToMonitorEvent(MonitorEvent *event, pgsocket fd);
extern void NotifyMonitorEvent(void);

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
 * а если сделать реверсивно - подписчики, и каждому подписчику добавлять битовую маску сигналов, которую ему нужно отправлять?
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
 * 2 варианта
 * 1) event = bit, eventset = bitmask, тогда создать битову маску 
 * нетрудозатратно, и 
 * 2) event = number, eventset = array*, тогда создание битовой маски - дело пропащее,
 * а значит, 
 * 
 * а почему вообще эта штука должна создавать сокет? с какой стати это ее ответственность?
 * пользователю надо - пусть он его и создает, и все проблемы!
 * тогда в любом случае - какой бы eventset ни был, не нужно создавать сокеты
 * 
 * Если пользователь будет все равно сам создавать сокет, то можно и не делать штуку с сетом событий
 * Можно подписываться как на сет, так и на одно событие (можно такой интерфейс добавить в целом)
 * Подписываться на сет имеет смысл сразу, если там в этом сете будет какая-то экстра информация об обработке событий, etc
 * 
 * тогда в описании subscriber должно быть поле socket 
 * 
 * в целом, если пока сет не нужен и пользователь будет передавать свой файловый дескриптор, то можно
 * пока что сет и не создавать
 * 
 */

typedef struct MonitorSubscriber
{
    pgsocket	fd;	
    /* Тут бы по хорошему иметь еще какую-то метаинформацию */
} MonitorSubscriber;

typedef struct MonitorSubscription
{
    /* flag needed for clearing array of subscriprions*/
    bool is_active;
    
    MonitorSubscriber subscriber;

} MonitorSubscription;

typedef MonitorSubscription*  MonitorSubscription_Ref;

typedef struct EventToSubscriberEntry {
    /*
     * There would be question if (MonitorEvent) or (MonitorEvent *)
     * just bc of where to keep this structure.
     */
    MonitorEvent event;

    /* number of subscribers to this kind of event*/
    int nsubscribtion;
    int max_nsubscriptions;

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



#endif                          /* MONITOR_EVENT_H */
