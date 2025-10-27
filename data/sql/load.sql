TRUNCATE TABLE conversion;
COPY conversion 
FROM 'data/bitso/conversion.csv' 
(DELIMITER ',', HEADER);

TRUNCATE TABLE earnings;
COPY earnings 
FROM 'data/bitso/earnings.csv'
(DELIMITER ',', HEADER);

TRUNCATE TABLE funding;
COPY funding 
FROM 'data/bitso/funding.csv'
(DELIMITER ',', HEADER);

TRUNCATE TABLE trade;
COPY trade 
FROM 'data/bitso/trade.csv'
(DELIMITER ',', HEADER);

TRUNCATE TABLE withdrawal;
COPY withdrawal
FROM 'data/bitso/withdrawal.csv'
(HEADER, DELIMITER ',');

TRUNCATE TABLE staging_transactions;
COPY staging_transactions 
FROM 'data/bitso/transactions.csv'
(DELIMITER ',', HEADER);

