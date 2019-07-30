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

ch_res_t ch_select(ch_client_t chc, const char *query, ch_select_cb cb, void *ctx) {
    Client *cl = (Client*)chc;
    cl->SelectCancelable(query, [=](const Block&b){ return cb(ctx, (ch_block_t)&b);});
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

int ch_is_i(ch_col_t col) {
    switch(ch_col_type(col)) {
        case Type::Int8: case Type::UInt8: return 8;
        case Type::Int16: case Type::UInt16: return 16;
        case Type::Int32: case Type::UInt32: return 32;
        case Type::Int64: case Type::UInt64: return 64;
    }
    return 0;
}

int ch_is_f(ch_col_t col) {
    switch(ch_col_type(col)) {
        case Type::Float32: return 32;
        case Type::Float64: return 64;
    }
    return 0;
} 

int ch_is_s(ch_col_t col) {
    switch(ch_col_type(col)) {
        case Type::String: return -1;
        case Type::FixedString: return ((ColumnFixedString*)col)->FixedSize();
    }
    return 0;    
}

int ch_is_e(ch_col_t col) {
    switch(ch_col_type(col)) {
        case Type::Enum8: return 8;
        case Type::Enum16: return 16;
    }
    return 0;    
}

const char* ch_col_name(ch_block_t blk, int col) {
    Block*block = (Block*)blk;
    return block->GetColumnName(col).c_str();
}

ch_res_t ch_get_i(int64_t* result, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    *result = 0;
    #define CHC(ct, tt) case Type::ct: *result = ((ColumnVector<tt>*)col)->At(nrow); break;
    switch(ct) {
        CHC(Int8, int8_t)
        CHC(Int16, int16_t)
        CHC(Int32, int32_t)
        CHC(Int64, int64_t)
        CHC(UInt8, uint8_t)
        CHC(UInt16, uint16_t)
        CHC(UInt32, uint32_t)
        CHC(UInt64, uint64_t)
        case Type::Enum8: *result = ((ColumnEnum8*)col)->At(nrow); break;
        case Type::Enum16: *result = ((ColumnEnum16*)col)->At(nrow); break;
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

    #define CHC(ct, tt) case Type::ct: *result = ((ColumnVector<tt>*)col)->At(nrow); break;
    switch(ct) {        
        CHC(Float32, float)
        CHC(Float64, double)
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
        case Type::DateTime: *result = ((ColumnDateTime*)col)->At(nrow); break;
        case Type::Date: *result =  ((ColumnDate*)col)->At(nrow); break;
        default: return 1;
    }
    return 0;
}

ch_res_t ch_get_s_len(size_t* result, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    *result = 0;
    switch(ct) {
        case Type::String: *result = ((ColumnString*)col)->At(nrow).size(); break;
        case Type::FixedString: *result = ((ColumnFixedString*)col)->At(nrow).size(); break;
        case Type::Enum8: *result = ((ColumnEnum8*)col)->NameAt(nrow).size(); break;
        case Type::Enum16: *result = ((ColumnEnum16*)col)->NameAt(nrow).size();break;
        default:  {
            *result=32;  // enough for doulbe/guid/etc
            return 1;
        }
    }
    return 0;

}

ch_res_t ch_get_s(char* result, size_t *size, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    size_t cap = *size;
    int n;
    std::string str;
    *result = 0;
    switch(ct) {
        case Type::String: str = ((ColumnString*)col)->At(nrow); strncpy(result, str.c_str(), cap); *size=str.length(); break;
        case Type::FixedString: str = ((ColumnFixedString*)col)->At(nrow); strncpy(result, str.c_str(), cap); *size=str.length(); break;
        case Type::Enum8: str = ((ColumnEnum8*)col)->NameAt(nrow); strncpy(result, str.c_str(), cap); *size=str.length();  break;
        case Type::Enum16: str = ((ColumnEnum16*)col)->At(nrow); strncpy(result, str.c_str(), cap); *size=str.length();  break;
        default:  {
            double f;
            int64_t i;
            time_t t;
            if(!ch_get_i(&i, col, nrow)) {
                snprintf(result, cap,"%" PRIi64, i);
                return 0;
            }else if(!ch_get_f(&f, col, nrow)) {
                n=snprintf(result, cap, "%g", f);
                *size = n>=0 ? n:0;
                return 0;
            }else if(!ch_get_t(&t, col, nrow)) {
                struct tm* info;
                char*ptr=result;
                info = localtime( &t );
                n = strftime(ptr, cap, ct==Type::Date ? "%Y-%m-%d" : "%Y-%m-%d %H:%M:%S", info);
                *size = n>=0 ? n:0;
                return 0;
            }
            return 1;
        }
    }
    return 0;
}

ch_res_t ch_get_s_ptr(const char** result, ch_col_t col, int nrow) {
    int ct = ch_col_type(col);
    *result = 0;
    switch(ct) {
        case Type::String: *result = ((ColumnString*)col)->At(nrow).c_str(); break;
        case Type::FixedString: *result = ((ColumnFixedString*)col)->At(nrow).c_str(); break;
        default:  return 1;
    }
    return 0;
}

ch_res_t ch_append_f(ch_col_t col, double value) {
    int ct = ch_col_type(col);
    
    #define CHC(ct, tt) case Type::ct: ((ColumnVector<tt>*)col)->Append(value); break;
    switch(ct) {        
        CHC(Float32, float)
        CHC(Float64, double)
        default:
            return ch_append_i(col, (int64_t)value);
    }
    #undef CHC
    return 0;
}

ch_res_t ch_append_tt(ch_col_t col, time_t value) {
    int ct = ch_col_type(col);
    switch(ct) {
        case Type::DateTime: ((ColumnDateTime*)col)->Append(value); break;
        case Type::Date: ((ColumnDate*)col)->Append(value); break;
        default: 
            assert(false);
            return -1;
    }
    return 0;
}

ch_res_t ch_append_s(ch_col_t col, const char* value) {
    int ct = ch_col_type(col);
    switch(ct) {
        case Type::String: ((ColumnString*)col)->Append(value); break;
        case Type::FixedString: ((ColumnFixedString*)col)->Append(value); break;
        case Type::Enum8: ((ColumnEnum8*)col)->Append(std::string(value)); break;
        case Type::Enum16: ((ColumnEnum16*)col)->Append(std::string(value)); break;
        default:  
            assert(false);
            return -1;
    }
    return 0;
}

ch_res_t ch_append_i(ch_col_t col, int64_t value) {
    int ct = ch_col_type(col);
    #define CHC(ct, tt) case Type::ct: ((ColumnVector<tt>*)col)->Append(value); break;
    switch(ct) {
        CHC(Int8, int8_t)
        CHC(Int16, int16_t)
        CHC(Int32, int32_t)
        CHC(Int64, int64_t)
        CHC(UInt8, uint8_t)
        CHC(UInt16, uint16_t)
        CHC(UInt32, uint32_t)
        CHC(UInt64, uint64_t)
        case Type::Enum8: ((ColumnEnum<int8_t>*)col)->Append(value);break;
        case Type::Enum16: ((ColumnEnum<int16_t>*)col)->Append(value);break;
        default:
            assert(false);
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

static bool consume(const char* &s, const char*tok) {
    const char* t = tok;
    const char* p = s;
    //printf("s:%s; tok:%s\n", s, tok);
    while(*p && 
    *t && 
    *p==*t) {
        p++; t++;
    }
    if(*t==0) {
        s = p;
        return true;
    }
    return false;
}
static bool consume_int(const char* s, int&i) {
    return sscanf(s, "%d", &i)==1;
}

static bool ch_parse_col_desc(const char*col_desc, Type::Code &code, int &len, std::vector<Type::EnumItem> & enums) {
    const char*p=col_desc;
    code = Type::Void;
    bool isEnum8;
    #define CONS(x) if(consume(p, #x)) { code= Type::x; return true;} 
    CONS(Int8)
    CONS(Int16)
    CONS(Int32)
    CONS(Int64)
    CONS(UInt8)
    CONS(UInt16)
    CONS(UInt32)
    CONS(UInt64)
    CONS(Float32)
    CONS(Float64)
    CONS(String)
    if(consume(p, "FixedString")) {
        code = Type::FixedString;
        if(!consume(p, "("))
            return false;
        const char*e=p;
        while(*e && *e!=')')
            e++;
       if(!consume_int(p,len))
            return false;
    }
    CONS(DateTime)
    CONS(Date)
    CONS(Array)
    CONS(Nullable)
    CONS(Tuple)
    if((isEnum8=consume(p, "Enum8")) || consume(p, "Enum16")) {
        code = isEnum8 ? Type::Enum8:Type::Enum16;
        if(!consume(p,"(")) return false;
        while(*p) {
            if(!consume(p,"'")) return false;
            const char*e=p;
            while(*e && *e!='\'')
                e++;
            if(*e==0)
                return false;
            std::string name(p, e-p);
            p=e+1;
            if(*p!='=') return false;
            p++;
            e=p;
            while(*e && *e!=',' && *e!=')')
                e++;
            int id = 0;    
            if(!consume_int(p, id))
                return false;
            p=e;
            //printf("%s %d %c\n", name.c_str(), id, *p);
            enums.emplace_back(Type::EnumItem{name, (int16_t)id});

            if(*p==')')
                break;
            else if(*p!=',')
                return false;
            p++;
        } 
        return true;
    }
    CONS(UUID)
    return false;
}

#define CHNC(ct, tt) case Type::ct: col = ColumnRef(new tt); break;

ch_col_t ch_col_new(ch_block_t blk, const char* col_name, const char*col_desc) {
    ColumnRef col;
    int len = 0;
    Type::Code code = Type::Void;
    std::vector<Type::EnumItem> enums;
    if(!ch_parse_col_desc(col_desc, code, len, enums))
        return NULL;
    switch(code) {
        CHNC(Date, ColumnDate());
        CHNC(DateTime, ColumnDateTime())
        CHNC(Int8, ColumnVector<int8_t>())
        CHNC(Int16, ColumnVector<int16_t>())
        CHNC(Int32, ColumnVector<int32_t>())
        CHNC(Int64, ColumnVector<int64_t>())
        CHNC(UInt8,  ColumnVector<uint8_t>())
        CHNC(UInt16,  ColumnVector<uint16_t>())
        CHNC(UInt32,  ColumnVector<uint32_t>())
        CHNC(UInt64,  ColumnVector<uint64_t>())
        CHNC(Float32, ColumnVector<float>())
        CHNC(Float64, ColumnVector<double>())
        CHNC(String, ColumnString())
        CHNC(FixedString, ColumnFixedString(len))
        CHNC(Enum8, ColumnEnum<int8_t>(Type::CreateEnum8(enums)))
        CHNC(Enum16, ColumnEnum<int16_t>(Type::CreateEnum16(enums)));
        default: 
            printf("Bad code %d\n", code);
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
    block->RefreshRowCount();
    client->Insert(table_name, *block);
    return 0;
}
