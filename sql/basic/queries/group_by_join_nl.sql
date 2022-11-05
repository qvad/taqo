SELECT t1.k1, t1.k2, t2.v1, t2.v2 FROM t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2 WHERE t1.k1 > %(1200) AND t1.k1 <= %(1300) GROUP BY t1.k1, t1.k2, t2.v1, t2.v2;
SELECT t1.k1, t1.k2, t2.v1, t2.v2 FROM t1 JOIN t2 ON t1.k1 = t2.k1 AND t1.k2 = t2.k2 WHERE t1.k1 > %(1200) AND t1.k1 <= %(1500) GROUP BY t1.k1, t1.k2, t2.v1, t2.v2;
SELECT ts2.k1, ts2.k2, ts3.v1, ts3.v2 FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 AND ts2.k2 = ts3.k2 WHERE ts2.k1 > %(1200) AND ts2.k1 <= %(1300) GROUP BY ts2.k1, ts2.k2, ts3.v1, ts3.v2;
SELECT ts2.k1, ts2.k2, ts3.v1, ts3.v2 FROM ts2 JOIN ts3 ON ts2.k1 = ts3.k1 AND ts2.k2 = ts3.k2 WHERE ts2.k1 > %(1200) AND ts2.k1 <= %(1400) GROUP BY ts2.k1, ts2.k2, ts3.v1, ts3.v2;