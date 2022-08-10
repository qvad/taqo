SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2 WHERE t1.k1 > 1200 AND t1.k1 <= 1300;
SELECT * FROM t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2 WHERE t1.k1 > 1200 AND t1.k1 <= 1500;
SELECT * FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 AND ts2.k2 = ts3.k2 WHERE ts2.k1 > 1200 AND ts2.k1 <= 1300;
SELECT * FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 AND ts2.k2 = ts3.k2 WHERE ts2.k1 > 1200 AND ts2.k1 <= 1400;