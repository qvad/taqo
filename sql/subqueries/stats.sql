select (select count(*) from t1000000),
       (select count(*) from t500000),
       (select count(*) from t50000),
       (select count(*) from t100);