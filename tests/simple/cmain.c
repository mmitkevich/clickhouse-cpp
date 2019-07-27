#include <chlib.h>
#include <cilog.h>
#include <stdio.h>


int select_cb(ch_block_t blk) {
    int ncols = ch_blk_ncols(blk);
    int nrows = ch_blk_nrows(blk);
    int r,c;
    printf("nrows: %d, ncols: %d\n", nrows, ncols);
    for(c=0;c<ncols;c++){
        printf("%8s | ",ch_col_name(blk, c));
    }
    printf("\n--------------------------\n");
    for(r=0; r<nrows; r++) {
        for(c=0; c<ncols; c++) {
            ch_col_t col = ch_blk_col(blk, c);
            char buf[1024]="\0";
            if(0==ch_get_s(buf, sizeof(buf), col, r))
                printf("%8s | ", buf);
            else
                printf("%8s | ", "???");
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
    ch_col_t date = ch_col_new(blk, ch_Date, "date");
    time_t now = time(NULL);
    ch_col_t price = ch_col_new(blk, ch_Float64, "price");
    ch_append_tt(date, now); 
    ch_append_f(price, 123.);
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