/*-------------------------------------------------------------------------
 *
 * monitor_event_types.h
 *	  Definitions related to monitoring events reporting
 *
 * src/include/utils/monitor_event.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONITOR_EVENT_TYPES_H
#define MONITOR_EVENT_TYPES_H

#include "postgres.h"

/*
 * Итак, как должно выглядеть событие?
 * 
 * Варианты
 * битовая маска
 * enum
 * некоторая структура
 * 
 * ...
 * Остановлюсь пока на enum, если что, его потом можно будут сочетать с битовой маской
*/

/*
 * Currently - just for an example
 * 
 * 
 */

typedef enum MonitorEvent {
    ME_A,
    ME_B,
    ME_C,
} MonitorEvent;

#define MONITOR_EVENT_NUM_TYPES ME_C + 1
#define MONITOR_EVENT_START ME_A

struct MonitorEventSet {
    pgsocket fd;

    int nevents;        /* number of registered events*/

    /*
     * Array, of nevents length, storing the definition of events
     * this set is waiting for.
     */
    MonitorEvent *events;
};


#endif                          /* MONITOR_EVENT_TYPES_H */