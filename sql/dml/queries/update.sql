-- tags: dml, update

update t1000000 set c_int = 1;
update t500000 set c_int = 1;
update t50000 set c_int = 1;
update t100 set c_int = 1;

update t1000000 set c_int = c_int + 1;
update t500000 set c_int = c_int + 1;
update t50000 set c_int = c_int + 1;
update t100 set c_int = c_int + 1;

update t1000000 set c_int = floor(c_float) + 1;
update t500000 set c_int = floor(c_float) + 1;
update t50000 set c_int = floor(c_float) + 1;
update t100 set c_int = floor(c_float) + 1;

update t1000000 set c_text = c_text || '_NEW';
update t500000 set c_text = c_text || '_NEW';
update t50000 set c_text = c_text || '_NEW';
update t100 set c_text = c_text || '_NEW';

update t1000000 set c_text = c_text || (select MAX(c_varchar) from t100);
update t500000 set c_text = c_text || (select MAX(c_varchar) from t100);
update t50000 set c_text = c_text || (select MAX(c_varchar) from t100);
update t100 set c_text = c_text || (select MAX(c_varchar) from t100);