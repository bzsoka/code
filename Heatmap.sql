-- Create table
Drop table brodie.bzs_arp_TABLA_ARCHIVALASI_DATUMOK;
create table brodie.bzs_arp_TABLA_ARCHIVALASI_DATUMOK
(
  owner      varchar2(100),
  table_name varchar2(100),
  col        VARCHAR2(100),
  year       NUMBER,
  db         NUMBER
)
tablespace DATA
  pctfree 10
  initrans 1
  maxtrans 255
  storage
  (
    initial 64K
    next 1M
    minextents 1
    maxextents unlimited
  );
  
  
  
  DECLARE
     cursor tablak is select 'BRUNO2'||t.OWNER||t.TABLE_NAME as tabla_id,t.OWNER,t.TABLE_NAME,t.COLUMN_NAME,t.DATA_TYPE,
                        'select '''||t.OWNER||''' as owner
                        , '''||t.TABLE_NAME||''' as table_name,'''||t.COLUMN_NAME||''' as col, EXTRACT(YEAR FROM t.'||t.COLUMN_NAME||') as year, count(*) as db 
                        from '||t.OWNER||'.'||t.TABLE_NAME||' t group by EXTRACT(YEAR FROM t.'||t.COLUMN_NAME||')' as v_query
                         from dba_tab_columns t 
                         --order by t.OWNER||t.TABLE_NAME
                         LEFT JOIN brodie.bzs_arp_TABLA_ARCHIVALASI_DATUMOK tad ON t.OWNER = tad.OWNER and t.TABLE_NAME  = tad.table_name and t.COLUMN_NAME = tad.col
                         ----------SZ�K�T�S--------
                         where t.owner in ('A2B_ABLAK','A2B_BRUNO','BRUNO_OWNER')
                         --('A_ABLAK','BOMA','INCA','LEVAL','SPL','A_CSOB','A_GFB','UNO','AKR','AKKRT','BR_MIG','BR_SEC','LEVAL_BR','BR_KTNY',
                         --'ABLAK','KOZPONT','BR_TRZS','BRUNO')
                         and t.data_type in
                         ('DATE','TIMESTAMP(3)','TIMESTAMP(6)','TIMESTAMP(2)','TIMESTAMP(2) WITH LOCAL TIME ZONE')
                         and t.TABLE_NAME not like 'V%' and t.TABLE_NAME not like 'RV%' and t.TABLE_NAME not like 'T_LOG%'
                         ----------SZ�K�T�S------------
                         and tad.col is null --tad.owner is null -- ha m�r megsz�moltuk �jra ne csin�ljuk meg
                         ;
     
     TYPE ARRAY IS TABLE OF brodie.bzs_arp_TABLA_ARCHIVALASI_DATUMOK%ROWTYPE;
     v_ARCH_DATUMOK ARRAY;

     
  BEGIN
    --execute immediate 'TRUNCATE TABLE TABLA_ARCHIVALASI_DATUMOK2';
     FOR tabla in tablak
     LOOP
       begin 
            
       execute immediate tabla.v_query BULK COLLECT INTO v_ARCH_DATUMOK;  
          FOR indx IN 1 .. v_ARCH_DATUMOK.COUNT
              LOOP
                   -- DBMS_OUTPUT.put_line (v_ARCH_DATUMOK (indx).col || ' ' || v_ARCH_DATUMOK (indx).year);
                    Insert into brodie.bzs_arp_TABLA_ARCHIVALASI_DATUMOK values v_ARCH_DATUMOK (indx);
                    commit;  
              END LOOP; 
        EXCEPTION
        WHEN OTHERS THEN
          dbms_output.put_line(tabla.tabla_id ||' '|| SQLERRM);
       end;
     end loop;
  END;
/

select * from brodie.bzs_arp_TABLA_ARCHIVALASI_DATUMOK; --131064


--HEatmap
select * from 
(select distinct 
       t.table_name,
       d.row_db, d.migr_flag_br2, d.f_mig_data, 
       d3.CREATED, d3.LAST_DDL_TIME, 
       --d.max_created, d.max_last_ddl_time, d.max_last_analyzed,
       round((d2.BYTES/1024/1024/1024),2) GB,
       --round(d.segment_mb/1024,2) GB,
       t.y_1950+t.y_1951+t.y_1952+t.y_1953+t.y_1954+t.y_1955+t.y_1956+t.y_1957+t.y_1958+t.y_1959+
       t.y_1960+t.y_1961+t.y_1962+t.y_1963+t.y_1964+t.y_1965+t.y_1966+t.y_1967+t.y_1968+t.y_1969+
       t.y_1970+t.y_1971+t.y_1972+t.y_1973+t.y_1974+t.y_1975+t.y_1976+t.y_1977+t.y_1978+t.y_1979+
       t.y_1980+t.y_1981+t.y_1982+t.y_1983+t.y_1984+t.y_1985+t.y_1986+t.y_1987+t.y_1988+t.y_1989+
       t.y_1990+t.y_1991+t.y_1992+t.y_1993+t.y_1994+t.y_1995+t.y_1996+t.y_1997+t.y_1998+t.y_1999 y_1950_1999,
       t.y_1950, t.y_1951, t.y_1952, t.y_1953, t.y_1954, t.y_1955, t.y_1956, t.y_1957, t.y_1958, t.y_1959, 
       t.y_1960, t.y_1961, t.y_1962, t.y_1963, t.y_1964, t.y_1965, t.y_1966, t.y_1967, t.y_1968, t.y_1969, 
       t.y_1970, t.y_1971, t.y_1972, t.y_1973, t.y_1974, t.y_1975, t.y_1976, t.y_1977, t.y_1978, t.y_1979, 
       t.y_1980, t.y_1981, t.y_1982, t.y_1983, t.y_1984, t.y_1985, t.y_1986, t.y_1987, t.y_1988, t.y_1989, 
       t.y_1990, t.y_1991, t.y_1992, t.y_1993, t.y_1994, t.y_1995, t.y_1996, t.y_1997, t.y_1998, t.y_1999,
       t.y_2000, t.y_2001, t.y_2002, t.y_2003, t.y_2004, t.y_2005, t.y_2006, t.y_2007, t.y_2008, t.y_2009,
       t.y_2010, t.y_2011, t.y_2012, t.y_2013, t.y_2014, t.y_2015, t.y_2016, t.y_2017, t.y_2018, t.y_2019,
       t.y_2020, t.y_2021, t.y_2022, t.y_2023, t.y_2024, t.y_2025, t.y_2026, t.y_2027, t.y_2028, t.y_2029,
       t.y_2030
from (
select t.owner||'.'||t.table_name as table_name,
SUM(DECODE(t.year, 1950, t.db)) y_1950,
SUM(DECODE(t.year, 1951, t.db)) y_1951,
SUM(DECODE(t.year, 1952, t.db)) y_1952,
SUM(DECODE(t.year, 1953, t.db)) y_1953,
SUM(DECODE(t.year, 1954, t.db)) y_1954,
SUM(DECODE(t.year, 1955, t.db)) y_1955,
SUM(DECODE(t.year, 1956, t.db)) y_1956,
SUM(DECODE(t.year, 1957, t.db)) y_1957,
SUM(DECODE(t.year, 1958, t.db)) y_1958,
SUM(DECODE(t.year, 1959, t.db)) y_1959,
SUM(DECODE(t.year, 1960, t.db)) y_1960,
SUM(DECODE(t.year, 1961, t.db)) y_1961,
SUM(DECODE(t.year, 1962, t.db)) y_1962,
SUM(DECODE(t.year, 1963, t.db)) y_1963,
SUM(DECODE(t.year, 1964, t.db)) y_1964,
SUM(DECODE(t.year, 1965, t.db)) y_1965,
SUM(DECODE(t.year, 1966, t.db)) y_1966,
SUM(DECODE(t.year, 1967, t.db)) y_1967,
SUM(DECODE(t.year, 1968, t.db)) y_1968,
SUM(DECODE(t.year, 1969, t.db)) y_1969,
SUM(DECODE(t.year, 1970, t.db)) y_1970,
SUM(DECODE(t.year, 1971, t.db)) y_1971,
SUM(DECODE(t.year, 1972, t.db)) y_1972,
SUM(DECODE(t.year, 1973, t.db)) y_1973,
SUM(DECODE(t.year, 1974, t.db)) y_1974,
SUM(DECODE(t.year, 1975, t.db)) y_1975,
SUM(DECODE(t.year, 1976, t.db)) y_1976,
SUM(DECODE(t.year, 1977, t.db)) y_1977,
SUM(DECODE(t.year, 1978, t.db)) y_1978,
SUM(DECODE(t.year, 1979, t.db)) y_1979,
SUM(DECODE(t.year, 1980, t.db)) y_1980,
SUM(DECODE(t.year, 1981, t.db)) y_1981,
SUM(DECODE(t.year, 1982, t.db)) y_1982,
SUM(DECODE(t.year, 1983, t.db)) y_1983,
SUM(DECODE(t.year, 1984, t.db)) y_1984,
SUM(DECODE(t.year, 1985, t.db)) y_1985,
SUM(DECODE(t.year, 1986, t.db)) y_1986,
SUM(DECODE(t.year, 1987, t.db)) y_1987,
SUM(DECODE(t.year, 1988, t.db)) y_1988,
SUM(DECODE(t.year, 1989, t.db)) y_1989,
SUM(DECODE(t.year, 1990, t.db)) y_1990,
SUM(DECODE(t.year, 1991, t.db)) y_1991,
SUM(DECODE(t.year, 1992, t.db)) y_1992,
SUM(DECODE(t.year, 1993, t.db)) y_1993,
SUM(DECODE(t.year, 1994, t.db)) y_1994,
SUM(DECODE(t.year, 1995, t.db)) y_1995,
SUM(DECODE(t.year, 1996, t.db)) y_1996,
SUM(DECODE(t.year, 1997, t.db)) y_1997,
SUM(DECODE(t.year, 1998, t.db)) y_1998,
SUM(DECODE(t.year, 1999, t.db)) y_1999,
SUM(DECODE(t.year, 2000, t.db)) y_2000,
SUM(DECODE(t.year, 2001, t.db)) y_2001,
SUM(DECODE(t.year, 2002, t.db)) y_2002,
SUM(DECODE(t.year, 2003, t.db)) y_2003,
SUM(DECODE(t.year, 2004, t.db)) y_2004,
SUM(DECODE(t.year, 2005, t.db)) y_2005,
SUM(DECODE(t.year, 2006, t.db)) y_2006,
SUM(DECODE(t.year, 2007, t.db)) y_2007,
SUM(DECODE(t.year, 2008, t.db)) y_2008,
SUM(DECODE(t.year, 2009, t.db)) y_2009,
SUM(DECODE(t.year, 2010, t.db)) y_2010,
SUM(DECODE(t.year, 2011, t.db)) y_2011,
SUM(DECODE(t.year, 2012, t.db)) y_2012,
SUM(DECODE(t.year, 2013, t.db)) y_2013,
SUM(DECODE(t.year, 2014, t.db)) y_2014,
SUM(DECODE(t.year, 2015, t.db)) y_2015,
SUM(DECODE(t.year, 2016, t.db)) y_2016,
SUM(DECODE(t.year, 2017, t.db)) y_2017,
SUM(DECODE(t.year, 2018, t.db)) y_2018,
SUM(DECODE(t.year, 2019, t.db)) y_2019,
SUM(DECODE(t.year, 2020, t.db)) y_2020,
SUM(DECODE(t.year, 2021, t.db)) y_2021,
SUM(DECODE(t.year, 2022, t.db)) y_2022,
SUM(DECODE(t.year, 2023, t.db)) y_2023,
SUM(DECODE(t.year, 2024, t.db)) y_2024,
SUM(DECODE(t.year, 2025, t.db)) y_2025,
SUM(DECODE(t.year, 2026, t.db)) y_2026,
SUM(DECODE(t.year, 2027, t.db)) y_2027,
SUM(DECODE(t.year, 2028, t.db)) y_2028,
SUM(DECODE(t.year, 2029, t.db)) y_2029,
SUM(DECODE(t.year, 2030, t.db)) y_2030
from 
(select b.*,  
        dense_rank() over(partition by b.table_name order by decode(b.owner,'BRUNO_OWNER',1,0) desc) dr
from brodie.bzs_arp_TABLA_ARCHIVALASI_DATUMOK b 
order by b.table_name, b.col, b.year, b.owner) t
where t.dr = 1
group by  t.owner||'.'||t.table_name) t 
LEFT OUTER JOIN brodie.bzs_arp_konv_teszt_bruno2_tablak_20191102_rdy d
ON t.table_name = d.owner_br2||'.'||d.table_name_br2
LEFT OUTER JOIN dba_segments d2
ON t.table_name = d2.owner ||'.'||d2.segment_name
LEFT OUTER JOIN dba_objects d3
ON t.table_name = d3.owner ||'.'||d3.object_name
where d3.OBJECT_TYPE = 'TABLE')
--where (gb >= 5 or row_db >= 50000000) --gb >= 5 or row_db >= 50000000
--      and substr(table_name,1,14) != 'BRUNO_OWNER.I_'
order by row_db desc nulls last;
