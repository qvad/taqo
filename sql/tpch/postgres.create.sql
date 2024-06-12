create table region
(
    r_regionkey integer primary key,
    r_name      varchar(25) not null,
    r_comment   varchar(152)
);

create table nation
(
    n_nationkey integer primary key,
    n_name      varchar(25) not null,
    n_regionkey integer references region (r_regionkey),
    n_comment   varchar(152)
);

create table part
(
    p_partkey     integer primary key,
    p_name        varchar(55),
    p_mfgr        varchar(25),
    p_brand       varchar(10),
    p_type        varchar(25),
    p_size        integer,
    p_container   varchar(10),
    p_retailprice decimal(15, 2),
    p_comment     varchar(23)
);

create table supplier
(
    s_suppkey   integer primary key,
    s_name      varchar(25),
    s_address   varchar(40),
    s_nationkey integer references nation (n_nationkey),
    s_phone     varchar(15),
    s_acctbal   decimal(15, 2),
    s_comment   varchar(101)
);

create table partsupp
(
    ps_partkey    integer references part (p_partkey),
    ps_suppkey    integer references supplier (s_suppkey),
    ps_availqty   integer,
    ps_supplycost decimal(15, 2),
    ps_comment    varchar(199),
    primary key (ps_partkey, ps_suppkey)
);

create table customer
(
    c_custkey    integer primary key,
    c_name       varchar(25),
    c_address    varchar(40),
    c_nationkey  integer references nation (n_nationkey),
    c_phone      varchar(15),
    c_acctbal    decimal(15, 2),
    c_mktsegment varchar(10),
    c_comment    varchar(117)
);

create table orders
(
    o_orderkey      integer primary key,
    o_custkey       integer references customer (c_custkey),
    o_orderstatus   char(1),
    o_totalprice    decimal(15, 2),
    o_orderdate     date,
    o_orderpriority char(15),
    o_clerk         char(15),
    o_shippriority  integer,
    o_comment       varchar(79)
);

create table lineitem
(
    l_orderkey      integer references orders (o_orderkey),
    l_partkey       integer references part (p_partkey),
    l_suppkey       integer references supplier (s_suppkey),
    l_linenumber    integer,
    l_quantity      decimal(15, 2),
    l_extendedprice decimal(15, 2),
    l_discount      decimal(15, 2),
    l_tax           decimal(15, 2),
    l_returnflag    char(1),
    l_linestatus    char(1),
    l_shipdate      date,
    l_commitdate    date,
    l_receiptdate   date,
    l_shipinstruct  char(25),
    l_shipmode      char(10),
    l_comment       varchar(44),
    primary key (l_orderkey, l_suppkey, l_partkey, l_linenumber)
);

create index idx_supplier_nation_key on supplier (s_nationkey);
create index idx_partsupp_partkey on partsupp (ps_partkey);
create index idx_partsupp_suppkey on partsupp (ps_suppkey);
create index idx_customer_nationkey on customer (c_nationkey);
create index idx_orders_custkey on orders (o_custkey);
create index idx_orders_orderdate on orders (o_orderdate);
create index idx_lineitem_orderkey on lineitem (l_orderkey);
create index idx_lineitem_part_supp on lineitem (l_partkey, l_suppkey);
create index idx_lineitem_shipdate on lineitem (l_shipdate, l_discount, l_quantity);
create index idx_nation_regionkey on nation (n_regionkey);