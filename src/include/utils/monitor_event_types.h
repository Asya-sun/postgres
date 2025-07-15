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


#endif                          /* MONITOR_EVENT_TYPES_H */