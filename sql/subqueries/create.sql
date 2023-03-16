CREATE TABLE t1000000
    WITH (colocation = true) AS
SELECT c_int,
       (case when c_int % 2 = 0 then true else false end) as c_bool,
       (c_int + 0.0001)::text as c_text,
        (c_int + 0.0002):: varchar as c_varchar,
        (c_int + 0.1):: decimal as c_decimal,
        (c_int + 0.2):: float as c_float,
        (c_int + 0.3):: real as c_real,
        (c_int + 0.4) ::money as c_money FROM generate_Series(1, 100000 * $MULTIPLIER) c_int;
CREATE INDEX t1000000_1_idx ON t1000000 (c_int);
CREATE INDEX t1000000_2_idx ON t1000000 (c_int, c_bool);
CREATE INDEX t1000000_3_idx ON t1000000 (c_int, c_text);
CREATE INDEX t1000000_4_idx ON t1000000 (c_int, c_varchar);
CREATE INDEX t1000000_5_idx ON t1000000 (c_float, c_text, c_varchar);
CREATE INDEX t1000000_6_idx ON t1000000 (c_float, c_decimal, c_varchar);
CREATE INDEX t1000000_7_idx ON t1000000 (c_float, c_real, c_money);

CREATE TABLE t500000
    WITH (colocation = true) AS
SELECT c_int,
       (case when c_int % 2 = 0 then true else false end) as c_bool,
       (c_int + 0.0001)::text as c_text,
       (c_int + 0.0002):: varchar as c_varchar,
       (c_int + 0.1):: decimal as c_decimal,
       (c_int + 0.2):: float as c_float,
       (c_int + 0.3):: real as c_real,
       (c_int + 0.4) ::money as c_money FROM generate_Series(1, 50000 * $MULTIPLIER) c_int;
CREATE INDEX t500000_1_idx ON t500000 (c_int);
CREATE INDEX t500000_2_idx ON t500000 (c_int, c_bool);
CREATE INDEX t500000_3_idx ON t500000 (c_int, c_text);
CREATE INDEX t500000_4_idx ON t500000 (c_int, c_varchar);
CREATE INDEX t500000_5_idx ON t500000 (c_float, c_text, c_varchar);
CREATE INDEX t500000_6_idx ON t500000 (c_float, c_decimal, c_varchar);
CREATE INDEX t500000_7_idx ON t500000 (c_float, c_real, c_money);

CREATE TABLE t50000
    WITH (colocation = true) AS
SELECT c_int,
       (case when c_int % 2 = 0 then true else false end) as c_bool,
       (c_int + 0.0001)::text as c_text,
        (c_int + 0.0002):: varchar as c_varchar,
        (c_int + 0.1):: decimal as c_decimal,
        (c_int + 0.2):: float as c_float,
        (c_int + 0.3):: real as c_real,
        (c_int + 0.4) ::money as c_money FROM generate_Series (1, 5000 * $MULTIPLIER) c_int;
CREATE INDEX t50000_1_idx ON t50000 (c_int);
CREATE INDEX t50000_2_idx ON t50000 (c_int, c_bool);
CREATE INDEX t50000_3_idx ON t50000 (c_int, c_text);
CREATE INDEX t50000_4_idx ON t50000 (c_int, c_varchar);
CREATE INDEX t50000_5_idx ON t50000 (c_float, c_text, c_varchar);
CREATE INDEX t50000_6_idx ON t50000 (c_float, c_decimal, c_varchar);
CREATE INDEX t50000_7_idx ON t50000 (c_float, c_real, c_money);

CREATE TABLE t100
    WITH (colocation = true) AS
SELECT c_int,
       (case when c_int % 2 = 0 then true else false end) as c_bool,
       (c_int + 0.0001)::text as c_text,
        (c_int + 0.0002):: varchar as c_varchar,
        (c_int + 0.1):: decimal as c_decimal,
        (c_int + 0.2):: float as c_float,
        (c_int + 0.3):: real as c_real,
        (c_int + 0.4) ::money as c_money FROM generate_Series (1, 10 * $MULTIPLIER) c_int;
CREATE INDEX t100_1_idx ON t100 (c_int);
CREATE INDEX t100_2_idx ON t100 (c_int, c_bool);
CREATE INDEX t100_3_idx ON t100 (c_int, c_text);
CREATE INDEX t100_4_idx ON t100 (c_int, c_varchar);
CREATE INDEX t100_5_idx ON t100 (c_float, c_text, c_varchar);
CREATE INDEX t100_6_idx ON t100 (c_float, c_decimal, c_varchar);
CREATE INDEX t100_7_idx ON t100 (c_float, c_real, c_money);