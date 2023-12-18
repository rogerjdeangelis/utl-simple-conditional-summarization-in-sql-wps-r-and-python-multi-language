%let pgm=utl-simple-conditional-summarization-in-sql-wps-r-and-python-multi-language;

Simple conditional summarization in sql wps r and python multi language

  SOLUTIONS

     1 wps sql hardcoded
     2 wps dynamic
     3 wps sql r
     4 wps python sql
     5 wps r native
       https://stackoverflow.com/users/1845721/z%c3%a9-loff

github
https://tinyurl.com/y36nc5xb
https://github.com/rogerjdeangelis/utl-simple-conditional-summarization-in-sql-wps-r-and-python-multi-language

stackoverflow
https://tinyurl.com/24mcyth4
https://stackoverflow.com/questions/77680787/r-dplyr-ifelse-by-group
/*               _     _
 _ __  _ __ ___ | |__ | | ___ _ __ ___
| `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
| |_) | | | (_) | |_) | |  __/ | | | | |
| .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
|_|
*/

/**************************************************************************************************************************/
/*                                        |                                  |                                            */
/*               INPUT                    |           PROCESS                |                OUTPUT                      */
/*                                        |                                  |                                            */
/*                                        |                                  |                                            */
/* V1   V2     V3    V4    V5    V6    V7 |   group by each v1, v2 and count |  V1  V2     V3    V4    V5    V6    V7     */
/*                                        |   the number of non-NA responses |                                            */
/*  5  2014     1     2     4     1     1 |   for each variable from v3 to v7|.  5 2014     1     2     2     2     2     */
/*  5  2014     .     5     2     1     1 |                                  |                                            */
/*                                        |   example for 5 2014             |                                            */
/*  6  2014     .     5     2     1     1 |   all have count except v3,      |   6 2014     1     1     2     2     2     */
/*  6  2014     1     .     2     1     8 |   which has a count of one       |                                            */
/*                                        |                                  |                                            */
/*  7  2014     2     6     1     3     8 |                                  |   7 2014     3     3     3     3     3     */
/*  7  2014     2     5     2     1     1 |                                  |                                            */
/*  7  2014     2     2     1     4     1 |                                  |                                            */
/*                                        |                                  |                                            */
/**************************************************************************************************************************/

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/
options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
  input v1 v2 v3 v4 v5 v6 v7 ;
cards;
5 2014 1 2 4 1 1
5 2014 . 5 2 1 1
6 2014 . 5 2 1 1
6 2014 1 . 2 1 8
7 2014 2 6 1 3 8
7 2014 2 5 2 1 1
7 2014 2 2 1 4 1
;;;;
run;quit;
/*                                  _   _                   _               _          _
/ | __      ___ __  ___   ___  __ _| | | |__   __ _ _ __ __| | ___ ___   __| | ___  __| |
| | \ \ /\ / / `_ \/ __| / __|/ _` | | | `_ \ / _` | `__/ _` |/ __/ _ \ / _` |/ _ \/ _` |
| |  \ V  V /| |_) \__ \ \__ \ (_| | | | | | | (_| | | | (_| | (_| (_) | (_| |  __/ (_| |
|_|   \_/\_/ | .__/|___/ |___/\__, |_| |_| |_|\__,_|_|  \__,_|\___\___/ \__,_|\___|\__,_|
             |_|                 |_|
*/

proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
options validvarname=any;
proc sql;
   create
     table sd1.want as
   select
     v1
    ,v2
    ,sum(not missing(v3)) as v3
    ,sum(not missing(v4)) as v4
    ,sum(not missing(v5)) as v5
    ,sum(not missing(v6)) as v6
    ,sum(not missing(v7)) as v7
   from
    sd1.have
   group
    by v1, v2
;quit;
');

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SD1.WANT total obs=3                                                                                                   */
/*                                                                                                                        */
/*  V1     V2     V3    V4    V5    V6    V7                                                                              */
/*                                                                                                                        */
/*   5    2014     1     2     2     2     2                                                                              */
/*   6    2014     1     1     2     2     2                                                                              */
/*   7    2014     3     3     3     3     3                                                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                             _                             _
|___ \  __      ___ __  ___    __| |_   _ _ __   __ _ _ __ ___ (_) ___
  __) | \ \ /\ / / `_ \/ __|  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __|
 / __/   \ V  V /| |_) \__ \ | (_| | |_| | | | | (_| | | | | | | | (__
|_____|   \_/\_/ | .__/|___/  \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___|
                 |_|                |___/
*/

proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

%array(_vs,values=v3-v7);

%put &_vsn;
%put &_vs7;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
options validvarname=any;
proc sql;
   create
     table sd1.want as
   select
     v1
    ,v2
    ,%do_over(_vs,phrase=%str(sum(not missing(?)) as ?),between=comma)
   from
    sd1.have
   gro up
    by v1, v2
;quit;
");

proc print data=sd1.want;
run;quit;

%arrayDelete(_vs);

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SD1.WANT total obs=3                                                                                                   */
/*                                                                                                                        */
/*  V1     V2     V3    V4    V5    V6    V7                                                                              */
/*                                                                                                                        */
/*   5    2014     1     2     2     2     2                                                                              */
/*   6    2014     1     1     2     2     2                                                                              */
/*   7    2014     3     3     3     3     3                                                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                                         _
|___ /  __      ___ __  ___   _ __   ___  __ _| |
  |_ \  \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
 ___) |  \ V  V /| |_) \__ \ | |    \__ \ (_| | |
|____/    \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                 |_|                        |_|
*/
proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

%array(_vs,values=v3-v7);

%utl_submit_wps64x("
libname sd1 'd:/sd1';
options validvarname=any;
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want<-sqldf('
   select
      v1
     ,v2
     ,%do_over(_vs,phrase=%str(count(?) as ?),between=comma)
   from
     have
   group
     by v1, v2
');
want;
endsubmit;
import data=sd1.want r=want;
");

proc print data=sd1.want;
run;quit;

%arrayDelete(_vs);

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS R System                                                                                                      */
/*                                                                                                                        */
/*    V1   V2 v3 v4 v5 v6 v7                                                                                              */
/*  1  5 2014  1  2  2  2  2                                                                                              */
/*  2  6 2014  1  1  2  2  2                                                                                              */
/*  3  7 2014  3  3  3  3  3                                                                                              */
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  V1     V2     V3    V4    V5    V6    V7                                                                              */
/*                                                                                                                        */
/*   5    2014     1     2     2     2     2                                                                              */
/*   6    2014     1     1     2     2     2                                                                              */
/*   7    2014     3     3     3     3     3                                                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*  _                                      _   _                             _
| || |   __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
| || |_  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
|__   _|  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
   |_|     \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                  |_|         |_|    |___/                                |_|
*/

proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

%array(_vs,values=v3-v7);

%utl_submit_wps64x("
libname sd1 'd:/sd1';
options validvarname=any;
proc python;
export data=sd1.have python=have;
submit;
from os import path;
import pandas as pd;
import xport;
import xport.v56;
import pyreadstat;
import numpy as np;
import pandas as pd;
from pandasql import sqldf;
mysql = lambda q: sqldf(q, globals());
from pandasql import PandaSQL;
pdsql = PandaSQL(persist=True);
sqlite3conn = next(pdsql.conn.gen).connection.connection;
sqlite3conn.enable_load_extension(True);
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
mysql = lambda q: sqldf(q, globals());
want = pdsql('''
   select
      v1
     ,v2
     ,%do_over(_vs,phrase=%str(count(?) as ?),between=comma)
   from
     have
   group
     by v1, v2
''');
print(want);
pyreadstat.write_xport(want, 'd:/xpt/want.xpt',table_name='want',file_format_version=5);
endsubmit;
run;quit;
");

libname xpt xport "d:/xpt/want.xpt";
proc contents data=xpt._all_;
run;quit;
data want;
  set xpt.want;
run;quit;
proc print;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/* The WPS PYTHON Procedure                                                                                               */
/*                                                                                                                        */
/*     V1      V2  v3  v4  v5  v6  v7                                                                                     */
/* 0  5.0  2014.0   1   2   2   2   2                                                                                     */
/* 1  6.0  2014.0   1   1   2   2   2                                                                                     */
/* 2  7.0  2014.0   3   3   3   3   3                                                                                     */
/*                                                                                                                        */
/*                                                                                                                        */
/* The WPS Procedure                                                                                                      */
/*                                                                                                                        */
/*  V1     V2     V3    V4    V5    V6    V7                                                                              */
/*                                                                                                                        */
/*   5    2014     1     2     2     2     2                                                                              */
/*   6    2014     1     1     2     2     2                                                                              */
/*   7    2014     3     3     3     3     3                                                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___                                            _   _
| ___|  __      ___ __  ___   _ __   _ __   __ _| |_(_)_   _____
|___ \  \ \ /\ / / `_ \/ __| | `__| | `_ \ / _` | __| \ \ / / _ \
 ___) |  \ V  V /| |_) \__ \ | |    | | | | (_| | |_| |\ V /  __/
|____/    \_/\_/ | .__/|___/ |_|    |_| |_|\__,_|\__|_| \_/ \___|
                 |_|
*/

%utl_submit_wps64x("
libname sd1 'd:/sd1';
options validvarname=any;
proc r;
export data=sd1.have r=have;
submit;
library(dplyr);
want <- have %>%
  group_by(V1, V2) %>%
  summarise_at(vars(V3:V7), ~ sum(!is.na(.x)));
want;
endsubmit;
import data=sd1.want r=want;
");

proc print data=sd1.want;
run;quit;


/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS R System                                                                                                      */
/*                                                                                                                        */
/*    V1   V2 v3 v4 v5 v6 v7                                                                                              */
/*  1  5 2014  1  2  2  2  2                                                                                              */
/*  2  6 2014  1  1  2  2  2                                                                                              */
/*  3  7 2014  3  3  3  3  3                                                                                              */
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  V1     V2     V3    V4    V5    V6    V7                                                                              */
/*                                                                                                                        */
/*   5    2014     1     2     2     2     2                                                                              */
/*   6    2014     1     1     2     2     2                                                                              */
/*   7    2014     3     3     3     3     3                                                                              */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
