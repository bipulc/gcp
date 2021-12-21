CREATE OR REPLACE PROCEDURE bq_demo_ds.pop_demo_table_10c(t DATE, iter INT64)
BEGIN 
 
 -- Populate a set of rows into demo_table_10c table.
 -- Read the rows from an existing partition. take a random number of rows, increment date / tstamp column and insert into the table.
 
 DECLARE ts DATE;
 DECLARE X INT64 DEFAULT 0;
 DECLARE R FLOAT64 DEFAULT 0.1;
 
 -- SET ts = (SELECT MAX(EXTRACT(DATE FROM tstamp)) from bq_demo_ds.demo_table_10c); 
 
 SET ts = t;
 SET R = (SELECT RAND());
 
 LOOP
    SET X = X + 1;
    
    IF X < iter THEN
        
        INSERT INTO `data-analytics-bk.bq_demo_ds.demo_table_10c`
        (cust_identifier, tstamp, country_code, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr9, attr10)
        SELECT cust_identifier, TIMESTAMP_ADD(tstamp, INTERVAL X DAY), country_code, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr9, attr10
        FROM `data-analytics-bk.bq_demo_ds.demo_table_10c`
        WHERE EXTRACT(DATE FROM tstamp) = ts and rand() < R;

    ELSE
        BREAK;
    END IF;
 
 END LOOP;
END;
