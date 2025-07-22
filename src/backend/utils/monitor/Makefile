#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/monitor
#
# IDENTIFICATION
#    src/backend/utils/monitor/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/monitor
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	monitor_event.o

# When I start doing this for Windows
# # Флаги для libev
# ifeq ($(PORTNAME), win32)
#     # Windows
#     EV_LIB = -lev
# else
#     # Linux/Unix
#     EV_LIB = -lev
# endif


# # This for current version (on Linux)
# # Прямое указание путей к libev
# EV_CFLAGS = -I/usr/local/include
# EV_LIB = -L/usr/local/lib -lev

# # Добавляем флаги
# override CPPFLAGS += $(EV_CFLAGS)
# SHLIB_LINK += $(EV_LIB)

# # Добавляем флаги линковки
# SHLIB_LINK += $(EV_LIB)

include $(top_srcdir)/src/backend/common.mk
