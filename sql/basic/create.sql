CREATE OR REPLACE FUNCTION random_between(low INT, high INT)
    RETURNS INT AS
$$
BEGIN
    RETURN floor(random() * (high - low + 1) + low);
END;
$$ language 'plpgsql' STRICT;

CREATE TABLE t1 WITH (colocated = true) AS
    SELECT k1_int                                            as k1,
           k1_int::varchar                                   as k2,
           k1_int                                            as v1,
           array_to_string(
                   array(
                           select substr(
                                          'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                                          trunc(random_between(1, k1_int) % 62)::integer + 1, 1)
                           FROM generate_series(1, 16)), '') as v2
    FROM generate_Series(1, 500000) k1_int;
ALTER TABLE t1
    ADD CONSTRAINT t1_pk PRIMARY KEY (k1 ASC, k2 ASC);
CREATE INDEX ON t1 (v1 ASC, k2 ASC);

CREATE TABLE t2 WITH (colocated = true) AS
    SELECT k1_int                                             as k1,
           k1_int::varchar                                    as k2,
           k1_int                                             as v1,
           array_to_string(
                   array(
                           select substr(
                                          'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                                          trunc(random_between(1, k1_int) % 62)::integer + 1, 1)
                           FROM generate_series(1, 128)), '') as v2
    FROM generate_Series(1, 500000) k1_int;
ALTER TABLE t2
    ADD CONSTRAINT t2_pk PRIMARY KEY (k1 ASC, k2 ASC);
CREATE INDEX ON t2 (v1 ASC, k2 ASC);

CREATE TABLE t3 WITH (colocated = true) AS
    SELECT k1_int                                             as k1,
           k1_int::varchar                                    as k2,
           k1_int                                             as v1,
           array_to_string(
                   array(
                           select substr(
                                          'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                                          trunc(random_between(1, k1_int) % 62)::integer + 1, 1)
                           FROM generate_series(1, 512)), '') as v2
    FROM generate_Series(1, 500000) k1_int;
ALTER TABLE t3
    ADD CONSTRAINT t3_pk PRIMARY KEY (k1 ASC, k2 ASC);
CREATE INDEX ON t3 (v1 ASC, k2 ASC);

CREATE TABLE ts2 WITH (colocated = true) AS
    SELECT k1_int                                            as k1,
           k1_int::varchar                                   as k2,
           k1_int                                            as v1,
           array_to_string(
                   array(
                           select substr(
                                          'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                                          trunc(random_between(1, k1_int) % 62)::integer + 1, 1)
                           FROM generate_series(1, 16)), '') as v2
    FROM generate_Series(1, 20000) k1_int;
ALTER TABLE ts2
    ADD CONSTRAINT ts2_pk PRIMARY KEY (k1 DESC, k2 DESC);
CREATE INDEX ON ts2 (v1 ASC, k2 ASC);
UPDATE ts2 SET v1 = NULL WHERE k1 > 170000 and k1 <= 180000;
UPDATE ts2 SET v2 = NULL WHERE k1 > 180000;
UPDATE ts2 SET v1 = NULL WHERE k1 > 190000;

CREATE TABLE ts3 WITH (colocated = true) AS
    SELECT k1_int                                            as k1,
           k1_int::varchar                                   as k2,
           k1_int                                            as v1,
           array_to_string(
                   array(
                           select substr(
                                          'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                                          trunc(random_between(1, k1_int) % 62)::integer + 1, 1)
                           FROM generate_series(1, 16)), '') as v2
    FROM generate_Series(1, 50000) k1_int;
ALTER TABLE ts3
    ADD CONSTRAINT ts3_pk PRIMARY KEY (k1 DESC);
CREATE INDEX ON ts3 (v1 ASC, k2 ASC);
UPDATE ts3 SET v1 = NULL WHERE k1 > 20000 and k1 <= 30000;
UPDATE ts3 SET v2 = NULL WHERE k1 > 30000;
UPDATE ts3 SET v1 = NULL WHERE k1 > 40000;