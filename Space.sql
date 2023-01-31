select b.owner, b.segment_name, b.partition_name, b.segment_type, b.tablespace_name,
  b.bytes/1024/1024 segment_MB, b.blocks segment_blocks, 
  b.extents segment_extents, b3.num_rows table_rows, b3.blocks table_blocks,
  b2.CREATED, b2.LAST_DDL_TIME,b3.last_analyzed,
  regexp_substr (b.segment_name, '*2[[:digit:]][[:digit:]][[:digit:]]*') date_in_segmentname,
  b3.partitioned, b3.compression, b3.compress_for, b3.dropped
from  dba_segments b,--user_segments
  dba_objects b2, --all_objects --user_objects  
  dba_all_tables b3 --all_tables --user_all_tables
where /*b.segment_type in ('TABLE','TABLE PARTITION') --csak t�bl�kra val� sz�r�skor
        and b.owner = 'DATAWH� --csak a DATAWH objektumai
  and*/ b.owner = b2.OWNER(+)
  and b.owner = 'DATAWH'
  and b.segment_name = b2.OBJECT_NAME(+)
  and nvl(b.partition_name,'X') = nvl(b2.SUBOBJECT_NAME(+),'X')
  and b.owner = b3.owner(+) 
  and b.segment_name = b3.table_name(+)
;