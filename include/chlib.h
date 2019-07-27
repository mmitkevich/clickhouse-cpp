#ifndef __CHLIB_H__
#define __CHLIB_H__
#pragma once
#ifdef __cplusplus
extern "C" {
#endif
#include <stdint.h>
#include <time.h>

#include <cilog.h>

struct ch_client{void *x;};
struct ch_block{void *x;};
struct ch_res{void *x;};
struct ch_col{void *x;};

enum ch_column_code {
    ch_Void = 0,
    ch_Int8,
    ch_Int16,
    ch_Int32,
    ch_Int64,
    ch_UInt8,
    ch_UInt16,
    ch_UInt32,
    ch_UInt64,
    ch_Float32,
    ch_Float64,
    ch_String,
    ch_FixedString,
    ch_DateTime,
    ch_Date,
    ch_Array,
    ch_Nullable,
    ch_Tuple,
    ch_Enum8,
    ch_Enum16,
    ch_UUID,
};

typedef struct ch_client *ch_client_t;
typedef struct ch_block *ch_block_t;
typedef int ch_res_t;
typedef struct ch_col *ch_col_t;

struct ch_client_options {
    const char *host;
    uint32_t port;
    const char *user;
    const char *password;
};

typedef int (*ch_select_cb)(ch_block_t blk);


ch_res_t ch_client_new(struct ch_client_options *opts, ch_client_t *chc);

ch_res_t ch_client_free(ch_client_t chc);

ch_res_t ch_select(ch_client_t chc, const char *query, ch_select_cb);

ch_block_t ch_block_new();
void ch_block_free(ch_block_t);

ch_col_t ch_col_new(ch_block_t blk, const char* col_name, const char* col_desc);
void ch_col_free(ch_col_t);

ch_res_t ch_append_f(ch_col_t col, double value);
ch_res_t ch_append_tt(ch_col_t col, time_t value);
ch_res_t ch_append_s(ch_col_t col, const char* value);
ch_res_t ch_append_i(ch_col_t col, int64_t value);

ch_res_t ch_insert(ch_client_t cl, const char* table_name, ch_block_t blk);

size_t ch_blk_ncols(ch_block_t blk);
ch_col_t ch_blk_col(ch_block_t, int index);
size_t ch_blk_nrows(ch_block_t blk);
size_t ch_col_nrows(ch_col_t blk);
int ch_col_type(ch_col_t col);
const char* ch_col_name(ch_block_t blk, int index);

ch_res_t ch_get_i(int64_t* result, ch_col_t col, int nrow);
ch_res_t ch_get_f(double* result, ch_col_t col, int nrow);
ch_res_t ch_get_t(time_t *result, ch_col_t col, int nrow);
ch_res_t ch_get_s(char* result, size_t size, ch_col_t col, int nrow);

#ifdef __cplusplus
}
#endif
#endif //__CHLIB_H__