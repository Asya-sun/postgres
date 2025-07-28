# contrib/test_mes/Makefile

MODULE_big = test_mes
OBJS = \
	$(WIN32RES) \
	test_mes.o

EXTENSION = test_mes
DATA = test_mes--1.0.sql
PGFILEDESC = "test extention for testing MES"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/test_mes
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

