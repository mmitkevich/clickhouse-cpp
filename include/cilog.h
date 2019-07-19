#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#include <errno.h>

#ifndef CERRNO
#define CERRNO errno
#endif

#ifndef CLOGF
#define CLOGF stdout
#endif

#define CLOG(f, x, ...) fprintf(f, x, __VA_ARGS__) 
#define ILOG(x, ...) CLOG(CLOGF, x, __VA_ARGS__)
#define ELOG(x, ...) CLOG(CLOGF, x, __VA_ARGS__)

const char* cstrerr(int);
void cseterr(int errcode, const char* file, int line, const char* fmt, ...);

#define CSTRERR(errno) cstrerr(errno)
#define CFAIL(code, fail, ...) { cseterr(code, __FILE__, __LINE__, __VA_ARGS__ ); goto fail; } 

#define CTRY(expr, fail) { if(CERRNO=(expr)) CFAIL(CERRNO, fail, #expr) }


#define CCATCH(code, fail, TException) catch(const TException &e) CFAIL(code, fail, #TException ": %s", e.what())
#ifdef __cplusplus
}
#endif

