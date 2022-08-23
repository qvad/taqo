SELECT * FROM t1 JOIN t2 on t1.k1 = t2.k1 WHERE t1.k1 >= 2500 AND t1.k1 < 25100 AND t2.k1 >= 2500 AND t2.k1 < 25100;
SELECT * FROM t1 JOIN t2 on t1.k1 = t2.k1 WHERE t1.k1 >= 24800 AND t1.k1 < 25100 AND t2.k1 >= 2500 AND t2.k1 < 25300;
SELECT * FROM ts2 JOIN ts3 on ts2.k1 = ts3.k1 WHERE ts2.k1 >= 300 AND ts2.k1 < 3100 AND ts3.k1 >= 300 AND ts3.k1 < 3100;
SELECT * FROM ts2 JOIN ts3 on ts2.k1 = ts3.k1 WHERE ts2.k1 >= 2800 AND ts2.k1 < 3100 AND ts3.k1 >= 300 AND ts3.k1 < 3300;