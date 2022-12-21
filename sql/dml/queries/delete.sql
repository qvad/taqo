-- tags: dml, delete

delete from t1000000;
delete from t500000;
delete from t50000;
delete from t100;

delete from t1000000 where c_int < (select MAX(c_int) from t100);
delete from t500000 where c_int < (select MAX(c_int) from t100);
delete from t50000 where c_int < (select MAX(c_int) from t100);
delete from t100 where c_int < (select MAX(c_int) from t100);