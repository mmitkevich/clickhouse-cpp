#include "chlib.h"
#include <stdarg.h>
#include <inttypes.h>
#include <assert.h>

#include "clickhouse/client.h"
#include "clickhouse/query.h"
#include "clickhouse/columns/column.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/numeric.h"

using namespace clickhouse;

extern "C" {
    static char errmsg[4096];

    const char* cstrerr(int) {
        return errmsg;
    }

    void cseterr(int errcode, const char* file, int line, const char* fmt, ...) {
        CERRNO = errcode;
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(errmsg, sizeof(errmsg), fmt, ap);
        va_end(ap);
        ELOG( "%s(%d) at %s:%d\n",errmsg, errcode, file, line);
    }
}

ch_res_t ch_client_new(struct ch_client_options*opts, ch_client_t *chc) {
    ClientOptions clopts;
    if(opts->host) {
        clopts.SetHost(opts->host);
    }
    if(opts->password)
        clopts.SetPassword(opts->password);
    if(opts->port)
        clopts.SetPort(opts->port);
    if(opts->user)
        clopts.SetUser(opts->user);
    *chc = NULL;
    try {
        *chc = (ch_client_t) new Client(clopts); 
    } catch(std::system_error &e) {
        CFAIL(ECONNREFUSED, fail, e.what());
    }catch(...) CFAIL(EFAULT, fail, "...");

    return 0;
fail:
    return errno;
}

ch_res_t ch_client_free(ch_client_t chc) {
    delete chc;
    return 0;
}

ch_res_t ch_select(ch_client_t chc, const char *query, ch_select_cb cb) {
    Client *cl = (Client*)chc;
    cl->SelectCancelable(query, (bool (*)(const Block&))cb);
    return 0;
}

#define CH_IMPL_GET(res_t, suffix, ColumnT) \
    res_t ch_get_##suffix(ch_col_t col, int row) { \
        return ((ColumnT*)col)->At(row); \
    }

CH_IMPL_GET(time_t, time, ColumnDate)
CH_IMPL_GET(int32_t, int32, ColumnInt32)
CH_IMPL_GET(int64_t, int64, ColumnInt64)
CH_IMPL_GET(double, double, ColumnFloat64)

size_t ch_blk_nrows(ch_block_t blk) {
    return ((Block*)blk)->GetRowCount();
}

size_t ch_blk_ncols(ch_block_t blk) {
    return ((Block*)blk)->GetColumnCount();
}

ch_col_t ch_blk_col(ch_block_t blk, int col) {
    Column* c = (*(Block*)blk)[col].get();
    return (ch_col_t)c;
}

int ch_col_type(ch_col_t col) {
    Column* column = (Column*)col;
    TypeRef type = column->Type();
    Type::Code tc =  type->GetCode();
    return (int)tc;
}

const char* ch_col_name(ch_block_t blk, int col) {
    Block*block = (Block*)blk;
    return block->GetColumnName(col).c_str();
}

ch_res_t ch_get_i(int64_t* result, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    *result = 0;
    #define CHC(ct, tt) case ct: *result = ((ColumnVector<tt>*)col)->At(nrow); break;
    switch(ct) {
        CHC(ch_Int8, int8_t)
        CHC(ch_Int16, int16_t)
        CHC(ch_Int32, int32_t)
        CHC(ch_Int64, int64_t)
        CHC(ch_UInt8, uint8_t)
        CHC(ch_UInt16, uint16_t)
        CHC(ch_UInt32, uint32_t)
        CHC(ch_UInt64, uint64_t)
        default:
            return -1;
    }
    #undef CHC
    return 0;
}

ch_res_t ch_get_f(double* result, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    int64_t i64 = 0;
    
    *result = 0;

    #define CHC(ct, tt) case ct: *result = ((ColumnVector<tt>*)col)->At(nrow); break;
    switch(ct) {        
        CHC(ch_Float32, float)
        CHC(ch_Float64, double)
        default:
            if(0 ==ch_get_i(&i64, col, nrow))
                *result = (double)i64;
            else
                return 1;
    }
    #undef CHC
    return 0;
}

ch_res_t ch_get_t(time_t *result, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    *result = 0;
    switch(ct) {
        case ch_DateTime: *result = ((ColumnDateTime*)col)->At(nrow); break;
        case ch_Date: *result =  ((ColumnDate*)col)->At(nrow); break;
        default: return 1;
    }
    return 0;
}

ch_res_t ch_get_s(char* result, size_t size, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    *result = 0;
    switch(ct) {
        case ch_String: strncpy(result, ((ColumnString*)col)->At(nrow).c_str(), size); break;
        case ch_FixedString: strncpy(result, ((ColumnFixedString*)col)->At(nrow).c_str(), size); break;
        default:  {
            double f;
            int64_t i;
            time_t t;
            if(!ch_get_i(&i, col, nrow)) {
                snprintf(result, size,"%" PRIi64, i);
                return 0;
            }else if(!ch_get_f(&f, col, nrow)) {
                snprintf(result, size, "%g", f);
                return 0;
            }else if(!ch_get_t(&t, col, nrow)) {
                struct tm* info;
                char*ptr=result;
                info = localtime( &t );
                strftime(ptr, size, ct==ch_Date ? "%Y-%m-%d" : "%Y-%m-%d %H:%M:%S", info);
                return 0;
            }
            return 1;
        }
    }
    return 0;
}

ch_res_t ch_append_f(ch_col_t col, double value) {
    int ct = ch_col_type(col);
    
    #define CHC(ct, tt) case ct: ((ColumnVector<tt>*)col)->Append(value); break;
    switch(ct) {        
        CHC(ch_Float32, float)
        CHC(ch_Float64, double)
        default:
            return ch_append_i(col, (int64_t)value);
    }
    #undef CHC
    return 0;
}

ch_res_t ch_append_tt(ch_col_t col, time_t value) {
    int ct = ch_col_type(col);
    switch(ct) {
        case ch_DateTime: ((ColumnDateTime*)col)->Append(value); break;
        case ch_Date: ((ColumnDate*)col)->Append(value); break;
        default: return 1;
    }
    return 0;
}

ch_res_t ch_append_s(ch_col_t col, const char* value) {
    int ct = ch_col_type(col);
    switch(ct) {
        case ch_String: ((ColumnString*)col)->Append(value); break;
        case ch_FixedString: ((ColumnFixedString*)col)->Append(value); break;
        default:  return -1;
    }
    return 0;
}

ch_res_t ch_append_i(ch_col_t col, int64_t value) {
    int ct = ch_col_type(col);
    #define CHC(ct, tt) case ct: ((ColumnVector<tt>*)col)->Append(value); break;
    switch(ct) {
        CHC(ch_Int8, int8_t)
        CHC(ch_Int16, int16_t)
        CHC(ch_Int32, int32_t)
        CHC(ch_Int64, int64_t)
        CHC(ch_UInt8, uint8_t)
        CHC(ch_UInt16, uint16_t)
        CHC(ch_UInt32, uint32_t)
        CHC(ch_UInt64, uint64_t)
        default:
            return -1;
    }
    #undef CHC
    return 0;
}


ch_block_t ch_block_new() {
    return (ch_block_t)new Block();
}

void ch_block_free(ch_block_t blk) {
    delete (Block*)blk;
}

#define CHNC(ct, tt) case ct: col = ColumnRef(new tt); break;

ch_col_t ch_col_new(ch_block_t blk, ch_column_code code, const char* col_name) {
    ColumnRef col;
    switch(code) {
        CHNC(ch_Date, ColumnDate());
        CHNC(ch_DateTime, ColumnDateTime())
        CHNC(ch_Int8, ColumnVector<int8_t>())
        CHNC(ch_Int16, ColumnVector<int16_t>())
        CHNC(ch_Int32, ColumnVector<int32_t>())
        CHNC(ch_Int64, ColumnVector<int64_t>())
        CHNC(ch_UInt8,  ColumnVector<uint8_t>())
        CHNC(ch_UInt16,  ColumnVector<uint16_t>())
        CHNC(ch_UInt32,  ColumnVector<uint32_t>())
        CHNC(ch_UInt64,  ColumnVector<uint64_t>())
        CHNC(ch_Float32, ColumnVector<float>())
        CHNC(ch_Float64, ColumnVector<double>())
        default: 
            assert(false);
            return NULL;
    }
    Block* block = (Block*) blk;
    block->AppendColumn(col_name, col);
    return (ch_col_t)col.get();
}

ch_res_t ch_insert(ch_client_t cl, const char* table_name, ch_block_t blk) {
    Client *client = (Client*) cl;
    Block *block = (Block*)blk;
    client->Insert(table_name, *block);
    return 0;
}
