@echo off

IF "%1" EQU "" echo %0 file.c
IF "%1" NEQ "" cl /I.. %1 ../crossdb.lib
