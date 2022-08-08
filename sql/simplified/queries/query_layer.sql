SELECT t / 100 FROM generate_series(1, 1000000) AS t ORDER BY  t % 100;
SELECT SUM(t % 100), count(t / 100) FROM generate_series(1, 1000000) AS t;
SELECT |/ t, 'foo ' || t, now() FROM generate_series(1, 1000000) AS t;