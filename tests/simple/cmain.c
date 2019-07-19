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

const char *query = "SELECT * FROM taq WHERE date>=today() ORDER BY date, client_ts, client_ts_nanos LIMIT 100";

int main() {
    ch_client_t chc = NULL;
    struct ch_client_options opts = {
        .host = "localhost",
    };
    printf("query: %s\n", query);
    CTRY(ch_client_new(&opts, &chc), fail);
    ch_select(chc, query, select_cb);
    return 0;
fail:
    return errno;
}