drop table brodie.mezok_default_value_20191102 purge; 
drop table brodie.mezok_constraints_20191102_a purge; 
drop table brodie.mezok_constraints_20191102 purge; 

--drop table brodie.mezok_default_value_20191102 purge; 
create table brodie.mezok_default_value_20191102 compress as 
SELECT /*+parallel(10)*/ t.owner, t.table_name, t.column_name,
       EXTRACTVALUE(DBMS_XMLGEN.GETXMLTYPE
           ('select data_default from dba_tab_columns t2 where t2.owner='''
               || t.owner
               || ''' and t2.table_name='''
               || t.table_name
               || ''' and t2.column_name='''
               || t.column_name
               || '''')
         , '//text()') data_default
FROM   dba_tab_columns t
WHERE  t.owner NOT LIKE '%SYS%' AND t.owner NOT IN ('DBSNMP', 'XDB') AND t.default_length IS NOT NULL;
--4 sec

-----

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

----

create table brodie.mezok_constraints_20191102_a (owner varchar2(100), constraint_name varchar2(100), y clob );

----

declare
owner varchar2(100);
constraint_name varchar2(100);
l_clob clob;
begin
for x in (select owner, constraint_name from dba_constraints
  where owner not like '%SYS%' and owner not in ('DBSNMP','XDB') and constraint_type = 'C')
loop
insert into brodie.mezok_constraints_20191102_a values (x.owner, x.constraint_name, empty_clob()) returning y into l_clob;
long2clob( 'select search_condition from dba_constraints where constraint_name = :bv',
x.constraint_name,
l_clob );
end loop;
commit;
end;
--50 sec

----

--drop table brodie.mezok_constraints_20191102 purge; 
create table brodie.mezok_constraints_20191102 compress as 
select base.*,
       cols2.table_name r_table_name, cols2.column_name r_column_name, cols2.position r_position--, 
from 
(SELECT cons.owner, cons.constraint_name, cons.constraint_type, 
        cons.status, cons.search_condition,
        cons.table_name, cols.column_name, cols.position,         
        cons.r_owner, cons.r_constraint_name, cons2.status r_status
FROM
(SELECT con.owner, con.table_name, con.constraint_name, con.constraint_type, con.status,
       /*EXTRACTVALUE(DBMS_XMLGEN.GETXMLTYPE
           ('select search_condition from dba_constraints t2 where t2.owner='''
               || con.owner
               || ''' and t2.constraint_name='''
               || con.constraint_name
               || '''')
         , '//text()') search_condition,*/
        to_char(a.y) search_condition,
        con.r_owner, con.r_constraint_name
FROM dba_constraints con,
     brodie.mezok_constraints_20191102_a a
WHERE con.owner not like '%SYS%' and con.owner not in ('DBSNMP','XDB')
      and con.constraint_type = 'C'
      and con.owner = a.owner(+)
      and con.constraint_name = a.constraint_name(+)) cons
left outer join dba_cons_columns cols
on cons.owner = cols.owner
   AND cons.constraint_name = cols.constraint_name
left outer join dba_constraints cons2
on cons.r_owner = cons2.owner
   AND cons.r_constraint_name = cons2.constraint_name) base
left outer join dba_cons_columns cols2
on base.r_owner = cols2.owner
   AND base.r_constraint_name = cols2.constraint_name
   AND base.position = cols2.position
;
--11 sec

insert /*+append*/ into brodie.mezok_constraints_20191102
select base.*,
       cols2.table_name r_table_name, cols2.column_name r_column_name, cols2.position r_position--, 
from 
(SELECT cons.owner, cons.constraint_name, cons.constraint_type, 
        cons.status, null search_condition, --cons.search_condition,
        cons.table_name, cols.column_name, cols.position,         
        cons.r_owner, cons.r_constraint_name, cons2.status r_status
FROM
(SELECT con.owner, con.table_name, con.constraint_name, con.constraint_type, con.status,
       /*EXTRACTVALUE(DBMS_XMLGEN.GETXMLTYPE
           ('select search_condition from dba_constraints t2 where t2.owner='''
               || con.owner
               || ''' and t2.constraint_name='''
               || con.constraint_name
               || '''')
         , '//text()') search_condition,*/
       con.r_owner, con.r_constraint_name
FROM dba_constraints con
WHERE con.owner not like '%SYS%' and con.owner not in ('DBSNMP','XDB')
      and con.constraint_type != 'C') cons
left outer join dba_cons_columns cols
on cons.owner = cols.owner
   AND cons.constraint_name = cols.constraint_name
left outer join dba_constraints cons2
on cons.r_owner = cons2.owner
   AND cons.r_constraint_name = cons2.constraint_name) base
left outer join dba_cons_columns cols2
on base.r_owner = cols2.owner
   AND base.r_constraint_name = cols2.constraint_name
   AND base.position = cols2.position
;
commit;
--48sec

--ell
select count(1), count(distinct b.owner||b.constraint_name||b.column_name), 
       count(distinct b.owner||b.table_name||b.column_name) 
from brodie.mezok_constraints_20191102 b;
--40131	40131	36263

select * from brodie.mezok_constraints_20191102 b
where b.owner||b.constraint_name in
(select b.owner||b.constraint_name
from brodie.mezok_constraints_20191102 b
group by b.owner||b.constraint_name
having count(1) > 1)
order by b.owner||b.constraint_name
;
--ok

drop table brodie.mezok_constraints_20191102_a purge; 
