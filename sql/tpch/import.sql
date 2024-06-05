COPY region FROM '$DATA_PATH/region.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
COPY nation FROM '$DATA_PATH/nation.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
COPY part FROM '$DATA_PATH/part.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
COPY supplier FROM '$DATA_PATH/supplier.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
COPY partsupp FROM '$DATA_PATH/partsupp.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
COPY customer FROM '$DATA_PATH/customer.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
COPY orders FROM '$DATA_PATH/orders.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
COPY lineitem FROM '$DATA_PATH/lineitem.tbl' with (delimiter '|', FORMAT csv, NULL 'NULL');
