create table bzs_utas_kti_temp compress as 
select ktikod,
connect_by_root KTIKOD as legujabb_utodkti,
connect_by_root UGYNOKSEG as UGYNOKSEG,
connect_by_root UGYNOKSEG_tipus as UGYNOKSEG_tipus,
connect_by_root ertcsat as ertcsat
from ADATBANYASZAT_ERTCSAT
start with UTOD_KTIKOD is null
connect by prior KTIKOD=UTOD_KTIKOD
order siblings by KTIKOD;