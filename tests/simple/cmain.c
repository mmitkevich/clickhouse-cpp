#include <chlib.h>
#include <cilog.h>
#include <stdio.h>


int select_cb(ch_block_t blk) {
    int ncols = ch_blk_ncols(blk);
    int nrows = ch_blk_nrows(blk);
    int r,c;
    if(nrows==0)
        return 1;
    printf("nrows: %d, ncols: %d\n", nrows, ncols);
    for(c=0;c<ncols;c++){
        printf("%12s | ",ch_col_name(blk, c));
    }
    printf("\n--------------------------\n");
    for(r=0; r<nrows; r++) {
        for(c=0; c<ncols; c++) {
            ch_col_t col = ch_blk_col(blk, c);
            int64_t i=0;
            double d=0.;
            if(ch_is_f(col))
                ch_get_f(&d, col, r);
            else if(ch_is_i(col))
                ch_get_i(&i, col, r);
            else if(ch_is_e(col))
                ch_get_i(&i, col, r);
            char buf[1024]="\0";
            if(0==ch_get_s(buf, sizeof(buf), col, r))
                printf("%12s | ", buf);
            else
                printf("%12s | ", "???");
        }
        printf("\n");
    }
    return 1;
}

int test_select() {
    const char *query = "SELECT * FROM taq1 WHERE date>=today() ORDER BY date, client_ts, client_ts_nanos LIMIT 100";
    ch_client_t chc = NULL;
    struct ch_client_options opts = {
        .host = "localhost",
    };
    printf("query: %s\n", query);
    CTRY(ch_client_new(&opts, &chc), fail);
    ch_select(chc, query, select_cb);
    ch_client_free(chc);
fail:
    return errno;
}

int test_insert() {
    ch_client_t chc = NULL;
    struct ch_client_options opts = {
        .host = "localhost",
    };
    ch_block_t blk = ch_block_new();
    ch_col_t client_ts = ch_col_new(blk, "client_ts", "DateTime");
    ch_col_t client_ts_nanos = ch_col_new(blk, "client_ts_nanos", "UInt32");
    ch_col_t symbol = ch_col_new(blk, "symbol", "String");
    ch_col_t op = ch_col_new(blk, "op", "Enum8('insert'=1,'remove'=2,'snapshot'=3,'update'=4,'trade'=5)");
    ch_col_t price = ch_col_new(blk, "price", "Float64");
    
    time_t now = time(NULL);
    
    ch_append_tt(client_ts, now); 
    ch_append_f(price, 123.);
    ch_append_i(client_ts_nanos, 102030);
    ch_append_s(symbol, "ABC");
    ch_append_s(op, "remove");
    CTRY(ch_client_new(&opts, &chc), fail);
    CTRY(ch_insert(chc, "default.taq1", blk), fail);
    ch_client_free(chc);
fail:
    return errno;
}


int main() {
    CTRY(test_insert(), fail);
    CTRY(test_select(), fail);
    return 0;
fail:
    return errno;
}