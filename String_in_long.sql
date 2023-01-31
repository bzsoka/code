create or replace procedure long2clob( p_query in varchar2,
p_bindvar in varchar2,
p_clob in out clob )
as
l_cursor integer default dbms_sql.open_cursor;
l_long_val varchar2(20);
l_long_len number;
l_buflen number := 20;
l_curpos number := 0;
l_n number;
begin
dbms_sql.parse( l_cursor, p_query, dbms_sql.native );

dbms_sql.bind_variable( l_cursor, ':bv', p_bindvar );
dbms_sql.define_column_long(l_cursor, 1);
l_n := dbms_sql.execute(l_cursor);

if (dbms_sql.fetch_rows(l_cursor)>0)
then
loop
dbms_sql.column_value_long(l_cursor, 1, l_buflen, l_curpos ,
l_long_val, l_long_len );
exit when l_long_len = 0;
dbms_lob.write( p_clob, l_long_len, l_curpos+1, l_long_val );
l_curpos := l_curpos + l_long_len;
end loop;
end if;
dbms_sql.close_cursor(l_cursor);
exception
when others then
if dbms_sql.is_open(l_cursor) then
dbms_sql.close_cursor(l_cursor);
end if;
raise;
end long2clob;
/


-------------------------------------------------------------------------
--MVIEW keres�s
-------------------------------------------------------------------------
create table mview_search (owner varchar2(100), view_name varchar2(100), y clob );

declare
owner varchar2(100);
view_name varchar2(100);
l_clob clob;
begin
for x in ( select owner, mview_name from dba_mviews )
loop
insert into mview_search values ( x.owner, x.mview_name, empty_clob() ) returning y into l_clob;
long2clob( 'select query from dba_mviews where mview_name = :bv',
x.mview_name,
l_clob );
end loop;
commit;
end;
/
select * from mview_search where lower(y) like '%agndw%'
/

drop table mview_search purge;

-------------------------------------------------------------------------
-- VIEW kereses
-------------------------------------------------------------------------
create table view_search (owner varchar2(100), view_name varchar2(100), y clob );

declare
owner varchar2(100);
view_name varchar2(100);
l_clob clob;
begin
for x in ( select owner, view_name from dba_views )
loop
insert into view_search values ( x.owner, x.view_name, empty_clob() ) returning y into l_clob;
long2clob( 'select text from dba_views where view_name = :bv',
x.view_name,
l_clob );
end loop;
commit;
end;
/
select * from view_search where lower(y) like '%agndw%'
/

drop table view_search purge;
