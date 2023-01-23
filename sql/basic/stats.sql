SELECT (select max(k1) from t1),
       (select max(k1) from t2),
       (select max(k1) from t3),
       (select max(k1) from ts2),
       (select max(k1) from ts3);