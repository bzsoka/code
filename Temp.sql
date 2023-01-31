SELECT c.username "USER",
       c.osuser,
       c.sid,
       c.serial#,
       b.spid "UNIX_PID",
       c.machine,
       c.program "PROGRAM",
       a.blocks * e.block_size/1024/1024/1024 gb_temp_used,
       a.tablespace,
       d.sql_text
FROM v$sort_usage a, v$process b, v$session c, v$sqlarea d, dba_tablespaces e 
WHERE c.saddr=a.session_addr
      AND b.addr=c.paddr
      AND c.sql_address=d.address(+)
      AND a.tablespace = e.tablespace_name
GROUP BY c.username, c.osuser, c.sid, c.serial#, b.spid, c.machine, c.program, a.blocks,e.block_size,a.tablespace, d.sql_text
order by a.blocks * e.block_size/1024/1024 desc
