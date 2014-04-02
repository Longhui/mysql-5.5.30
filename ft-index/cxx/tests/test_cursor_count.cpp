/* -*- mode: C; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: expandtab:ts=8:sw=4:softtabstop=4:
/*
COPYING CONDITIONS NOTICE:

  This program is free software; you can redistribute it and/or modify
  it under the terms of version 2 of the GNU General Public License as
  published by the Free Software Foundation, and provided that the
  following conditions are met:

      * Redistributions of source code must retain this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below).

      * Redistributions in binary form must reproduce this COPYING
        CONDITIONS NOTICE, the COPYRIGHT NOTICE (below), the
        DISCLAIMER (below), the UNIVERSITY PATENT NOTICE (below), the
        PATENT MARKING NOTICE (below), and the PATENT RIGHTS
        GRANT (below) in the documentation and/or other materials
        provided with the distribution.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
  02110-1301, USA.

COPYRIGHT NOTICE:

  TokuDB, Tokutek Fractal Tree Indexing Library.
  Copyright (C) 2007-2013 Tokutek, Inc.

DISCLAIMER:

  This program is distributed in the hope that it will be useful, but
  WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  General Public License for more details.

UNIVERSITY PATENT NOTICE:

  The technology is licensed by the Massachusetts Institute of
  Technology, Rutgers State University of New Jersey, and the Research
  Foundation of State University of New York at Stony Brook under
  United States of America Serial No. 11/760379 and to the patents
  and/or patent applications resulting from it.

PATENT MARKING NOTICE:

  This software is covered by US Patent No. 8,185,551.

PATENT RIGHTS GRANT:

  "THIS IMPLEMENTATION" means the copyrightable works distributed by
  Tokutek as part of the Fractal Tree project.

  "PATENT CLAIMS" means the claims of patents that are owned or
  licensable by Tokutek, both currently or in the future; and that in
  the absence of this license would be infringed by THIS
  IMPLEMENTATION or by using or running THIS IMPLEMENTATION.

  "PATENT CHALLENGE" shall mean a challenge to the validity,
  patentability, enforceability and/or non-infringement of any of the
  PATENT CLAIMS or otherwise opposing any of the PATENT CLAIMS.

  Tokutek hereby grants to you, for the term and geographical scope of
  the PATENT CLAIMS, a non-exclusive, no-charge, royalty-free,
  irrevocable (except as stated in this section) patent license to
  make, have made, use, offer to sell, sell, import, transfer, and
  otherwise run, modify, and propagate the contents of THIS
  IMPLEMENTATION, where such license applies only to the PATENT
  CLAIMS.  This grant does not include claims that would be infringed
  only as a consequence of further modifications of THIS
  IMPLEMENTATION.  If you or your agent or licensee institute or order
  or agree to the institution of patent litigation against any entity
  (including a cross-claim or counterclaim in a lawsuit) alleging that
  THIS IMPLEMENTATION constitutes direct or contributory patent
  infringement, or inducement of patent infringement, then any rights
  granted to you under this License shall terminate as of the date
  such litigation is filed.  If you or your agent or exclusive
  licensee institute or order or agree to the institution of a PATENT
  CHALLENGE, then Tokutek may terminate any rights granted to you
  under this License.
*/

#ident "Copyright (c) 2007-2013 Tokutek Inc.  All rights reserved."
#ident "The technology is licensed by the Massachusetts Institute of Technology, Rutgers State University of New Jersey, and the Research Foundation of State University of New York at Stony Brook under United States of America Serial No. 11/760379 and to the patents and/or patent applications resulting from it."

#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <assert.h>
#include <db_cxx.h>
#include <memory.h>
#include <sys/stat.h>

int verbose;

#define FNAME __FILE__ ".tdb"

int keyeq(Dbt *a, Dbt *b) {
    if (a->get_size() != b->get_size()) return 0;
    return memcmp(a->get_data(), b->get_data(), a->get_size()) == 0;
}

int my_cursor_count(Dbc *cursor, db_recno_t *count, Db *db) {
    int r;
    Dbt key; key.set_flags(DB_DBT_REALLOC);
    Dbt val; val.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&key, &val, DB_CURRENT); assert(r == 0);
    
    Dbc *count_cursor;
    r = db->cursor(0, &count_cursor, 0); assert(r == 0);
    r = count_cursor->get(&key, &val, DB_SET); assert(r == 0);
    *count = 0;

    Dbt nkey, nval;
    for (;; ) {
        *count += 1;
        nkey.set_flags(DB_DBT_REALLOC);
        nval.set_flags(DB_DBT_REALLOC);
        r = count_cursor->get(&nkey, &nval, DB_NEXT);
        if (r != 0) break;
        if (!keyeq(&key, &nkey)) break;
    }
    r = 0;
    if (nkey.get_data()) toku_free(nkey.get_data());
    if (nval.get_data()) toku_free(nval.get_data());
    if (key.get_data()) toku_free(key.get_data());
    if (val.get_data()) toku_free(val.get_data());
    int rr = count_cursor->close(); assert(rr == 0);
    return r;
}

int my_next_nodup(Dbc *cursor, Dbt *key, Dbt *val) {
    int r;
    Dbt currentkey; currentkey.set_flags(DB_DBT_REALLOC);
    Dbt currentval; currentval.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&currentkey, &currentval, DB_CURRENT); assert(r == 0);
    Dbt nkey; nkey.set_flags(DB_DBT_REALLOC);
    Dbt nval; nval.set_flags(DB_DBT_REALLOC);
    for (;;) {
        r = cursor->get(&nkey, &nval, DB_NEXT);
        if (r != 0) break;
        if (!keyeq(&currentkey, &nkey)) break;
    }
    if (nkey.get_data()) toku_free(nkey.get_data());
    if (nval.get_data()) toku_free(nval.get_data());
    if (currentkey.get_data()) toku_free(currentkey.get_data());
    if (currentval.get_data()) toku_free(currentval.get_data());
    if (r == 0) r = cursor->get(key, val, DB_CURRENT);
    return r;
}

int my_prev_nodup(Dbc *cursor, Dbt *key, Dbt *val) {
    int r;
    Dbt currentkey; currentkey.set_flags(DB_DBT_REALLOC);
    Dbt currentval; currentval.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&currentkey, &currentval, DB_CURRENT); assert(r == 0);
    Dbt nkey; nkey.set_flags(DB_DBT_REALLOC);
    Dbt nval; nval.set_flags(DB_DBT_REALLOC);
    for (;;) {
        r = cursor->get(&nkey, &nval, DB_PREV);
        if (r != 0) break;
        if (!keyeq(&currentkey, &nkey)) break;
    }
    if (nkey.get_data()) toku_free(nkey.get_data());
    if (nval.get_data()) toku_free(nval.get_data());
    if (currentkey.get_data()) toku_free(currentkey.get_data());
    if (currentval.get_data()) toku_free(currentval.get_data());
    if (r == 0) r = cursor->get(key, val, DB_CURRENT);
    return r;
}

int my_next_dup(Dbc *cursor, Dbt *key, Dbt *val) {
    int r;
    Dbt currentkey; currentkey.set_flags(DB_DBT_REALLOC);
    Dbt currentval; currentval.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&currentkey, &currentval, DB_CURRENT); assert(r == 0);
    Dbt nkey; nkey.set_flags(DB_DBT_REALLOC);
    Dbt nval; nval.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&nkey, &nval, DB_NEXT);
    if (r == 0 && !keyeq(&currentkey, &nkey)) r = DB_NOTFOUND;
    if (nkey.get_data()) toku_free(nkey.get_data());
    if (nval.get_data()) toku_free(nval.get_data());
    if (currentkey.get_data()) toku_free(currentkey.get_data());
    if (currentval.get_data()) toku_free(currentval.get_data());
    if (r == 0) r = cursor->get(key, val, DB_CURRENT);
    return r;
}

int my_prev_dup(Dbc *cursor, Dbt *key, Dbt *val) {
    int r;
    Dbt currentkey; currentkey.set_flags(DB_DBT_REALLOC);
    Dbt currentval; currentval.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&currentkey, &currentval, DB_CURRENT); assert(r == 0);
    Dbt nkey; nkey.set_flags(DB_DBT_REALLOC);
    Dbt nval; nval.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&nkey, &nval, DB_PREV);
    if (r == 0 && !keyeq(&currentkey, &nkey)) r = DB_NOTFOUND;
    if (nkey.get_data()) toku_free(nkey.get_data());
    if (nval.get_data()) toku_free(nval.get_data());
    if (currentkey.get_data()) toku_free(currentkey.get_data());
    if (currentval.get_data()) toku_free(currentval.get_data());
    if (r == 0) r = cursor->get(key, val, DB_CURRENT);
    return r;
}

void load(Db *db, int n) {
    if (verbose) printf("load\n");
    int i;
    for (i=0; i<n; i++) {
        if (i == n/2) continue;
        int k = htonl(i);
        Dbt key(&k, sizeof k);
        int v = i;
        Dbt val(&v, sizeof v);
        int r = db->put(0, &key, &val, 0); assert(r == 0);
    }

    for (i=0; i<n; i++) {
        int k = htonl(n/2);
        int v = i;
        Dbt key(&k, sizeof k);
        Dbt val(&v, sizeof v);
        int r = db->put(0, &key, &val, 0); assert(r == 0);
    }
}

void test_cursor_count_flags(Db *db) {
    int r;
    Dbc *cursor;
    
    r = db->cursor(0, &cursor, 0); assert(r == 0);
    Dbt key; key.set_flags(DB_DBT_MALLOC);
    Dbt val; val.set_flags(DB_DBT_MALLOC);
    r = cursor->get(&key, &val, DB_FIRST); assert(r == 0);
    if (key.get_data()) toku_free(key.get_data());
    if (val.get_data()) toku_free(val.get_data());
    db_recno_t n;
    r = cursor->count(&n, 1); assert(r == EINVAL);
    r = cursor->count(&n, 0); assert(r == 0);
    if (verbose) printf("n=%d\n", n);
    r = cursor->close(); assert(r == 0);
}    

void walk(Db *db, int n) {
    if (verbose) printf("walk\n");
    Dbc *cursor;
    int r = db->cursor(0, &cursor, 0); assert(r == 0);
    Dbt key, val;
    int i;
    for (i=0;;i++) {
        key.set_flags(DB_DBT_REALLOC);
        val.set_flags(DB_DBT_REALLOC);
        r = cursor->get(&key, &val, DB_NEXT);
        if (r != 0) break;
        int k;
        assert(key.get_size() == sizeof k);
        memcpy(&k, key.get_data(), key.get_size());
        k = htonl(k);
        int v;
        assert(val.get_size() == sizeof v);
        memcpy(&v, val.get_data(), val.get_size());
        db_recno_t count;
        r = cursor->count(&count, 0); assert(r == 0);
        if (verbose) printf("%d %d %d\n", k, v, count);
#if 0
        db_recno_t mycount;
        r = my_cursor_count(cursor, &mycount, db); assert(r == 0);
        assert(mycount == count);
#endif
	assert(count==1); // we support only NODUP databases now.
    }
    toku_free(key.get_data());
    toku_free(val.get_data());
    r = cursor->close(); assert(r == 0);
}

int cursor_set(Dbc *cursor, int k) {
    Dbt key(&k, sizeof k);
    Dbt val;
    int r = cursor->get(&key, &val, DB_SET);
    return r;
}

void test_zero_count(Db *db, int n) {
    if (verbose) printf("test_zero_count\n");
    Dbc *cursor;
    int r = db->cursor(0, &cursor, 0); assert(r == 0);

    r = cursor_set(cursor, htonl(n/2)); assert(r == 0);
    db_recno_t count;
    r = cursor->count(&count, 0); assert(r == 0);
    assert((int)count == 1);

    Dbt key; key.set_flags(DB_DBT_REALLOC);
    Dbt val; val.set_flags(DB_DBT_REALLOC);
#if 0
    // c->del no longer supported.  See #4576.
    int i;
    for (i=1; count > 0; i++) {
        r = cursor->del(0); assert(r == 0);
        db_recno_t newcount;
        r = cursor->count(&newcount, 0);
        if (r != 0) 
            break;
        assert(newcount == count - 1);
        count = newcount;
        r = cursor->get(&key, &val, DB_NEXT);
        if (r != 0) break;
    }
    assert(i == 2);
#endif
    if (key.get_data()) toku_free(key.get_data());
    if (val.get_data()) toku_free(val.get_data());
    r = cursor->close(); assert(r == 0);
}

void test_next_nodup(Db *db, int n) {
    if (verbose) printf("test_next_nodup\n");
    int r;
    Dbc *cursor;
    r = db->cursor(0, &cursor, 0); assert(r == 0);
    Dbt key; key.set_flags(DB_DBT_REALLOC);
    Dbt val; val.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&key, &val, DB_FIRST); assert(r == 0);
    int i = 0;
    while (r == 0) {
        int k = htonl(*(int*)key.get_data());
        int v = *(int*)val.get_data();
        if (verbose) printf("%d %d\n", k, v);
        assert(k == i);
        if (k == n/2) assert(v == n-1); else assert(v == i);
        i += 1;
        // r = my_next_nodup(cursor, &key, &val);
        r = cursor->get(&key, &val, DB_NEXT_NODUP);
    }
    assert(i == n);
    if (key.get_data()) toku_free(key.get_data());
    if (val.get_data()) toku_free(val.get_data());
    r = cursor->close(); assert(r == 0);
}

void test_prev_nodup(Db *db, int n) {
    if (verbose) printf("test_prev_nodup\n");
    int r;
    Dbc *cursor;
    r = db->cursor(0, &cursor, 0); assert(r == 0);
    Dbt key; key.set_flags(DB_DBT_REALLOC);
    Dbt val; val.set_flags(DB_DBT_REALLOC);
    r = cursor->get(&key, &val, DB_LAST); assert(r == 0);
    int i = n-1;
    while (r == 0) {
        int k = htonl(*(int*)key.get_data());
        int v = *(int*)val.get_data();
        if (verbose) printf("%d %d\n", k, v);
        assert(k == i);
        if (k == n/2) assert(v == n-1); else assert(v == i);
        i -= 1;
        // r = my_next_nodup(cursor, &key, &val);
        r = cursor->get(&key, &val, DB_PREV_NODUP);
    }
    assert(i == -1);
    if (key.get_data()) toku_free(key.get_data());
    if (val.get_data()) toku_free(val.get_data());
    r = cursor->close(); assert(r == 0);
}

#define DIR "test_cursor_count.dir"

int main(int argc, char *argv[]) {
    for (int i=1; i<argc; i++) {
        char *arg = argv[i];
        if (strcmp(arg, "-v") == 0 || strcmp(arg, "--verbose") == 0)
            verbose = 1;
    }

    int r;

    system("rm -rf " DIR);
    toku_os_mkdir(DIR, 0777);

#if defined(USE_ENV) && USE_ENV
    DbEnv env(DB_CXX_NO_EXCEPTIONS);
    r = env.set_redzone(0); assert(r==0);
    r = env.open(DIR, DB_INIT_MPOOL + DB_CREATE + DB_PRIVATE, 0777); assert(r == 0);
    Db db(&env, DB_CXX_NO_EXCEPTIONS);
#else
    Db db(0, DB_CXX_NO_EXCEPTIONS);
#endif

    unlink(FNAME);
    r = db.open(0, FNAME, 0, DB_BTREE, DB_CREATE, 0777); assert(r == 0);
    
    load(&db, 10);
    test_cursor_count_flags(&db);
    walk(&db, 10);
    test_next_nodup(&db, 10);
    test_prev_nodup(&db, 10);
    test_zero_count(&db, 10);

    return 0;
}
