COPY t1 FROM '$DATA_PATH/t1.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');
COPY t2 FROM '$DATA_PATH/t2.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');
COPY t3 FROM '$DATA_PATH/t3.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');

COPY ts2 FROM '$DATA_PATH/ts2.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');
COPY ts3 FROM '$DATA_PATH/ts3.csv' with (delimiter ',', FORMAT csv, NULL 'NULL');