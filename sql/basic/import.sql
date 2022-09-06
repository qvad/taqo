COPY t1 FROM '$DATA_PATH/data/t1.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');
COPY t2 FROM '$DATA_PATH/data/t2.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');
COPY t3 FROM '$DATA_PATH/data/t3.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');

COPY ts2 FROM '$DATA_PATH/data/ts2.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');
COPY ts3 FROM '$DATA_PATH/data/ts3.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');