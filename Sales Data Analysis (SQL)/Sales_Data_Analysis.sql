DROP TABLE IF EXISTS sales_2019_table;
CREATE TABLE sales_2019_table (
	Order_ID VARCHAR(255),
	Product VARCHAR(255),
	Quantity VARCHAR(255),
	Price_Each VARCHAR(255),
	Order_Date VARCHAR(255),
	Purchase_Address VARCHAR(255)
)
DROP PROCEDURE IF EXISTS import_csv(file_name VARCHAR(255));
CREATE OR REPLACE PROCEDURE import_csv(file_name VARCHAR(255))
language plpgsql
as $$

DECLARE sales_2019_table TEXT; 

BEGIN

    sales_2019_table := 'sales_2019_table(Order_ID,Product,Quantity,Price_Each,Order_Date,Purchase_Address)'; 

        EXECUTE 'COPY ' || sales_2019_table || ' from ''' || file_name || ''' with csv header';

END $$;

CALL import_csv('C:\sampledb\Sales\Sales_January_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_February_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_March_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_April_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_May_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_June_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_July_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_August_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_September_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_October_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_November_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_December_2019.csv');
CALL import_csv('C:\sampledb\Sales\Sales_October_2019_new.csv');


SELECT * FROM sales_2019_table;

--Stored Procedure: Delete SQL Statement--
DROP PROCEDURE delete_row(character varying,character varying,character varying)
CREATE OR REPLACE PROCEDURE delete_row(table_name VARCHAR(255),column_name VARCHAR(255), delete_word VARCHAR(255))
language plpgsql
as $$

BEGIN	
	EXECUTE 'DELETE FROM ' || quote_ident(table_name) || ' WHERE ' || column_name || ' = ' || quote_literal(delete_word);
END $$; 

CALL delete_row('sales_2019_table','order_id', 'Order ID');

SELECT * FROM sales_2019_table;
-------------------------------------------

-------------Changing datatypes-------------
DROP PROCEDURE change_datatypes(character varying,character varying)
CREATE OR REPLACE PROCEDURE change_datatypes(column_name VARCHAR(255), data_type VARCHAR(255))
language plpgsql
as $$
BEGIN

	EXECUTE('ALTER TABLE sales_2019_table ALTER COLUMN '
	|| quote_ident(column_name)
	||' TYPE ' || data_type || ' USING '
	||quote_ident(column_name) || '::' || data_type);
END $$; 

SET datestyle = mdy;

CALL change_datatypes('order_id', 'INTEGER');
CALL change_datatypes('quantity', 'INTEGER');
CALL change_datatypes('price_each', 'NUMERIC(10,2)');
CALL change_datatypes('order_date', 'TIMESTAMP');

SELECT * FROM sales_2019_table;
---------------------------------------------

----------------------Creating of new table using Stored Procedure---------------------
CREATE OR REPLACE PROCEDURE data_versioning(new_table_name VARCHAR(100), current_table_name VARCHAR(100))
language plpgsql
as $$
BEGIN
    EXECUTE ('CREATE TABLE ' || quote_ident(new_table_name) || 
			 ' AS SELECT *' 
			 ' FROM ' || quote_ident(current_table_name));
END $$;

----------------------------------------------------------

--------------TRANSFERING TO STAGING--------------
DROP TABLE IF EXISTS sales_2019_staging;
CALL data_versioning('sales_2019_staging', 'sales_2019_table')

SELECT * FROM sales_2019_staging
WHERE order_date > '10/01/2019'
ORDER BY order_id ASC;
--------------------------------------------------

--------TRANSFORMATION PROCESS----------
----------DATA CLEANSING----------------
----------Removing of Null Values-------
---Data Versioning of No Null Values(Stored Procedure)---

DROP TABLE IF EXISTS sales_2019_nonull;
CALL data_versioning('sales_2019_nonull', 'sales_2019_staging');
SELECT * FROM sales_2019_nonull;
-------------------------------------------------------------
--Checking of Null Values--
SELECT * FROM sales_2019_nonull WHERE product IS NULL;
SELECT * FROM sales_2019_nonull WHERE quantity IS NULL;
SELECT * FROM sales_2019_nonull WHERE price_each IS NULL;
SELECT * FROM sales_2019_nonull WHERE order_date IS NULL;
SELECT * FROM sales_2019_nonull WHERE purchase_address IS NULL;
----------------------------

CREATE OR REPLACE PROCEDURE removing_null_values(table_name VARCHAR(255))
language plpgsql
as $$
BEGIN
	EXECUTE('DELETE FROM ' || quote_ident(table_name) || ' WHERE NOT (' || quote_ident(table_name)  || ' IS NOT NULL)');
END $$;

DROP TABLE IF EXISTS sales_2019_staging;
CALL removing_null_values('sales_2019_nonull');
SELECT * FROM sales_2019_nonull WHERE order_id IS NULL;
SELECT * FROM sales_2019_nonull;
---------------------------------------

------------Removing of duplicates------------------------
SELECT DISTINCT * FROM sales_2019_nonull;

CREATE OR REPLACE PROCEDURE creating_table(new_table_name VARCHAR(255), current_table_name VARCHAR(255))
language plpgsql
as $$
BEGIN
	EXECUTE('CREATE TABLE ' || quote_ident(new_table_name) || '(LIKE ' || quote_ident(current_table_name) || ');');
END $$;

DROP TABLE IF EXISTS sales_2019_noduplicates;
CALL creating_table('sales_2019_noduplicates', 'sales_2019_nonull');

CREATE OR REPLACE PROCEDURE remove_duplicates(
	new_table_name VARCHAR(100),
	col1 VARCHAR(100), 
	col2 VARCHAR(100), 
	col3 VARCHAR(100), 
	col4 VARCHAR(100), 
	col5 VARCHAR(100), 
	col6 VARCHAR(100), 
	current_table_name VARCHAR(100))
language plpgsql
as $$
BEGIN

	EXECUTE ('INSERT INTO' || ' ' || quote_ident(new_table_name) || '(' || quote_ident(col1) || ',' || quote_ident(col2) || ','
			|| quote_ident(col3) || ',' || quote_ident(col4) || ',' || quote_ident(col5) || ',' || quote_ident(col6) || ' ' || ')' || 
			 'SELECT DISTINCT' || ' ' || quote_ident(col1) || ',' || quote_ident(col2) || ','
			|| quote_ident(col3) || ',' || quote_ident(col4) || ',' || quote_ident(col5) || ','
			|| quote_ident(col6) || ' ' || 'FROM' || ' ' || quote_ident(current_table_name));

END $$;

CALL remove_duplicates('sales_2019_noduplicates', 'order_id', 'product', 'quantity', 'price_each', 'order_date', 'purchase_address', 'sales_2019_nonull');

SELECT * FROM sales_2019_noduplicates;

-------Standardization--------
DROP TABLE IF EXISTS sales_2019_standardized;
CALL data_versioning('sales_2019_standardized', 'sales_2019_noduplicates');

UPDATE sales_2019_standardized
SET purchase_address = REPLACE (purchase_address, 'St', 'Street');

SELECT * FROM sales_2019_standardized;

---------PARSING-------------
DROP TABLE IF EXISTS sales_2019_parsing;
SELECT * FROM sales_2019_parsing;
CALL data_versioning('sales_2019_parsing', 'sales_2019_standardized')

ALTER TABLE sales_2019_parsing ADD COLUMN date date;
ALTER TABLE sales_2019_parsing ADD COLUMN time time;

UPDATE sales_2019_parsing
SET
    date = order_date::date,
    time = order_date::time;

CREATE OR REPLACE PROCEDURE concept_hierarchy_date(
	current_table VARCHAR(255),
	col VARCHAR(255)
)
language plpgsql
as $$
BEGIN
	EXECUTE('ALTER TABLE' || ' ' || quote_ident(current_table) || ' ' || 'ADD' || ' ' || quote_ident(col) || ' ' || 'INTEGER;');
END $$;

CALL concept_hierarchy_date('sales_2019_parsing', 'order_date_year');
CALL concept_hierarchy_date('sales_2019_parsing', 'order_date_quarter');
CALL concept_hierarchy_date('sales_2019_parsing', 'order_date_month');
CALL concept_hierarchy_date('sales_2019_parsing', 'order_date_week');
CALL concept_hierarchy_date('sales_2019_parsing', 'order_date_day');


CREATE OR REPLACE PROCEDURE parsing_date(dimension VARCHAR(255), col1 VARCHAR(255), col2 VARCHAR(255), col3 VARCHAR(255))
language plpgsql
as $$
BEGIN
	EXECUTE('UPDATE' || ' ' || quote_ident(dimension) || ' ' || 'SET' || ' ' || quote_ident(col1) 
		   || ' ' || '=' || ' ' || 'EXTRACT(' || ' ' || quote_ident(col2) || ' ' || 'from' || ' ' || quote_ident(col3) ||');');
END $$;

CALL parsing_date('sales_2019_parsing', 'order_date_year','year', 'date');
CALL parsing_date('sales_2019_parsing', 'order_date_quarter','quarter', 'date');
CALL parsing_date('sales_2019_parsing', 'order_date_month','month', 'date');
CALL parsing_date('sales_2019_parsing', 'order_date_week','week', 'date');
CALL parsing_date('sales_2019_parsing', 'order_date_day','day', 'date');

SELECT * FROM sales_2019_parsing 
ORDER BY order_id ASC;

CREATE OR REPLACE PROCEDURE parsing_address(current_table VARCHAR(255), col VARCHAR(255))
language plpgsql
as $$
BEGIN
	EXECUTE('ALTER TABLE' || ' ' || quote_ident(current_table) || ' ' || 'ADD' || ' ' || quote_ident(col) || ' ' || 'VARCHAR(255);');
END $$;

CALL parsing_address('sales_2019_parsing', 'street');
CALL parsing_address('sales_2019_parsing', 'city');
CALL parsing_address('sales_2019_parsing', 'state');
CALL parsing_address('sales_2019_parsing', 'zipcode');

UPDATE sales_2019_parsing SET street = trim(split_part(purchase_address::TEXT, ',', 1));
UPDATE sales_2019_parsing SET city = trim(split_part(purchase_address::TEXT, ',', 2));
UPDATE sales_2019_parsing SET state = trim(split_part(trim(split_part(purchase_address::TEXT, ',', 3))::TEXT, ' ', 1));
UPDATE sales_2019_parsing SET zipcode = trim(split_part(trim(split_part(purchase_address::TEXT, ',' , 3))::TEXT, ' ', 2));

SELECT * FROM sales_2019_parsing 
ORDER BY order_id ASC;


----Data Versioning for Transformed table---
DROP TABLE IF EXISTS sales_2019_transformed;
CALL data_versioning('sales_2019_transformed', 'sales_2019_parsing');

SELECT * FROM sales_2019_transformed 
WHERE order_id = '150502';

-------------LOADING TO DIMENSIONAL MODEL--------------
-------------CREATING DIMENSIONAL TABLES---------------
-------------Order Dimension---------------
SELECT DISTINCT order_id FROM sales_2019_transformed; 

DROP TABLE IF EXISTS order_dimension;
CREATE TABLE order_dimension(
	order_id INTEGER PRIMARY KEY
);

CREATE OR REPLACE PROCEDURE insert_into_dimension(table_name VARCHAR(255), col1 VARCHAR(255), current_table VARCHAR(255))
language plpgsql
as $$
BEGIN
	EXECUTE('INSERT INTO' || ' ' || quote_ident(table_name) || '(' || quote_ident(col1) || ')'
		   || 'SELECT DISTINCT' || ' ' || quote_ident(col1) || ' ' || 'FROM' || ' ' || quote_ident(current_table) 
		   || ' ' || 'ORDER BY' || ' ' || quote_ident(col1) || ' ' ||'ASC;');
END $$;

CALL insert_into_dimension('order_dimension', 'order_id', 'sales_2019_transformed');

SELECT * FROM order_dimension WHERE order_id = '150502';

-------------Product Dimension---------------
SELECT DISTINCT product FROM sales_2019_transformed;

DROP TABLE IF EXISTS product_dimension;

CREATE TABLE product_dimension(
	product_id SERIAL PRIMARY KEY,
	product VARCHAR(255)
	);

CALL insert_into_dimension('product_dimension',
						   'product', 'sales_2019_transformed');

SELECT * FROM product_dimension;
------------Order_date_dimension-------------------------
SELECT DISTINCT order_date FROM sales_2019_transformed 


DROP TABLE IF EXISTS order_date_dimension;
CREATE TABLE order_date_dimension(
	order_date_id SERIAL PRIMARY KEY,
	order_date TIMESTAMP,
	date DATE,
	time TIME,
	order_date_year INTEGER,
	order_date_quarter INTEGER,
	order_date_month INTEGER,
	order_date_week INTEGER,
	order_date_day INTEGER
);

INSERT INTO order_date_dimension(order_date, date, time, order_date_year, order_date_quarter, 
								 order_date_month, order_date_week, order_date_day
) 
SELECT DISTINCT order_date, date, time, order_date_year, order_date_quarter, 
order_date_month, order_date_week, order_date_day FROM sales_2019_transformed 
ORDER BY order_date ASC;


SELECT * FROM order_date_dimension;

------------Address_dimension--------------------------------
SELECT DISTINCT purchase_address FROM sales_2019_transformed;
DROP TABLE IF EXISTS address_dimension;
CREATE TABLE address_dimension(
	purchase_address_id SERIAL PRIMARY KEY,
	purchase_address VARCHAR (255),
	street VARCHAR(255),
	city VARCHAR(255),
	state VARCHAR(2),
	zipcode VARCHAR(5)
);


INSERT INTO address_dimension(purchase_address, street, city, state, zipcode) 
SELECT DISTINCT purchase_address, street, city, state, zipcode FROM sales_2019_transformed 
ORDER BY purchase_address ASC;

SELECT * FROM address_dimension WHERE city = 'Portland' ORDER BY purchase_address_id ASC;
--------------------------------------------------------------------------------------------


----------------Creating sales_2019_fact--------------------------------------
DROP TABLE IF EXISTS sales_2019_fact;

CREATE TABLE sales_2019_fact(
	order_id INTEGER, 
	product_id INTEGER, 
	order_date_id INTEGER, 
	purchase_address_id INTEGER, 
	quantity INTEGER, 
	price_each NUMERIC(10,2),
	revenue NUMERIC(10,2)
	);
	
INSERT INTO sales_2019_fact(order_id, product_id, order_date_id, purchase_address_id, quantity, price_each,revenue)
SELECT order_dimension.order_id, product_dimension.product_id, order_date_dimension.order_date_id, 
		address_dimension.purchase_address_id, sales_2019_transformed.quantity, sales_2019_transformed.price_each, 
		SUM(price_each*quantity) AS revenue
FROM sales_2019_transformed
	INNER JOIN product_dimension ON sales_2019_transformed.product = product_dimension.product
	INNER JOIN order_date_dimension ON sales_2019_transformed.order_date = order_date_dimension.order_date
	INNER JOIN address_dimension ON sales_2019_transformed.purchase_address = address_dimension.purchase_address
	INNER JOIN order_dimension ON sales_2019_transformed.order_id = order_dimension.order_id
GROUP BY order_dimension.order_id, product_dimension.product_id, order_date_dimension.order_date_id, 
		address_dimension.purchase_address_id, sales_2019_transformed.quantity, sales_2019_transformed.price_each
ORDER BY order_id ASC;

SELECT * FROM sales_2019_fact;

ALTER TABLE sales_2019_fact
	ADD CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES order_dimension (order_id),
	ADD CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES product_dimension (product_id),
	ADD CONSTRAINT fk_order_date_id FOREIGN KEY (order_date_id) REFERENCES order_date_dimension (order_date_id),
	ADD CONSTRAINT fk_purchase_address_id FOREIGN KEY (purchase_address_id) REFERENCES address_dimension (purchase_address_id);

SELECT * FROM sales_2019_fact;


-----------------------------DATA CUBE------------------------------------------------------

SELECT order_dimension.order_id, product_dimension.product, sales_2019_fact.price_each, sales_2019_fact.quantity,
order_date_dimension.order_date, address_dimension.purchase_address
FROM sales_2019_fact
	INNER JOIN order_dimension ON order_dimension.order_id = sales_2019_fact.order_id
	INNER JOIN product_dimension ON product_dimension.product_id = sales_2019_fact.product_id
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
	INNER JOIN address_dimension ON address_dimension.purchase_address_id = sales_2019_fact.purchase_address_id
WHERE order_date_dimension.order_date >= '01/01/2019' AND order_date_dimension.order_date <= '10/14/2019' 
AND product_dimension.product = 'AA Batteries (4-pack)' AND order_dimension.order_id = '150503';




----BY MONTH--
SELECT order_date_dimension.order_date_month, product_dimension.product, SUM(revenue) AS monthly_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
	INNER JOIN product_dimension ON product_dimension.product_id = sales_2019_fact.product_id
GROUP BY CUBE (order_date_dimension.order_date_month, product_dimension.product)
ORDER BY order_date_dimension.order_date_month

----BY QUARTER--
SELECT order_date_dimension.order_date_quarter, product_dimension.product, SUM(revenue) AS quarterly_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
	INNER JOIN product_dimension ON product_dimension.product_id = sales_2019_fact.product_id
GROUP BY CUBE (order_date_dimension.order_date_quarter, product_dimension.product)
ORDER BY order_date_dimension.order_date_quarter

----BY WEEK--
SELECT order_date_dimension.order_date_week, product_dimension.product, SUM(revenue) AS weekly_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
	INNER JOIN product_dimension ON product_dimension.product_id = sales_2019_fact.product_id
GROUP BY CUBE (order_date_dimension.order_date_week,  product_dimension.product)
ORDER BY order_date_dimension.order_date_week

------------------ROLLUP-------------------------------------------------------------------------
----BY MONTH---
SELECT order_date_dimension.order_date_month, SUM(revenue) AS monthly_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
GROUP BY ROLLUP (order_date_dimension.order_date_month)
ORDER BY order_date_dimension.order_date_month

----BY QUARTER--
SELECT order_date_dimension.order_date_quarter, SUM(revenue) AS quarterly_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
GROUP BY ROLLUP (order_date_dimension.order_date_quarter)
ORDER BY order_date_dimension.order_date_quarter

----BY WEEK--
SELECT order_date_dimension.order_date_week, SUM(revenue) AS weekly_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
GROUP BY ROLLUP (order_date_dimension.order_date_week)
ORDER BY order_date_dimension.order_date_week


SELECT product_dimension.product, order_date_dimension.order_date_quarter, SUM(revenue) AS total_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
	INNER JOIN product_dimension ON product_dimension.product_id = sales_2019_fact.product_id
WHERE order_date_dimension.order_date_quarter = 1 AND product_dimension.product LIKE '%Batteries%'
GROUP BY ROLLUP (product_dimension.product, order_date_dimension.order_date_quarter)

SELECT product_dimension.product, order_date_dimension.order_date_month, SUM(revenue) AS total_sales
FROM sales_2019_fact
	INNER JOIN order_date_dimension ON order_date_dimension.order_date_id = sales_2019_fact.order_date_id
	INNER JOIN product_dimension ON product_dimension.product_id = sales_2019_fact.product_id
WHERE order_date_dimension.order_date_month IN (1,2,3) AND product_dimension.product LIKE '%Batteries%'
GROUP BY ROLLUP (product_dimension.product, order_date_dimension.order_date_month)


COPY product_dimension TO 'C:\sampledb\albums.csv' DELIMITER ',' CSV HEADER;
COPY order_date_dimension TO 'C:\sampledb\albums.csv' DELIMITER ',' CSV HEADER;
COPY address_dimension TO 'C:\sampledb\albums.csv' DELIMITER ',' CSV HEADER;
COPY order_dimension TO 'C:\sampledb\albums.csv' DELIMITER ',' CSV HEADER;
COPY sales_2019_fact TO 'C:\sampledb\albums.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE sales_2019(
	order_id INTEGER, 
	product_id INTEGER, 
	product VARCHAR(255),
	order_date_id INTEGER, 
	order_date TIMESTAMP,
	date DATE,
	time TIME,
	order_date_year INTEGER,
	order_date_quarter INTEGER,
	order_date_month INTEGER,
	order_date_week INTEGER,
	order_date_day INTEGER,
	purchase_address_id INTEGER, 
	purchase_address VARCHAR (255),
	street VARCHAR(255),
	city VARCHAR(255),
	state VARCHAR(2),
	zipcode VARCHAR(5),
	quantity INTEGER, 
	price_each NUMERIC(10,2),
	revenue NUMERIC(10,2)
	);
	
SELECT * FROM sales_2019;

INSERT INTO sales_2019(order_id, product_id, product, order_date_id, order_date,
							 date, time, order_date_year, order_date_quarter,
							 order_date_month, order_date_week,
							 order_date_day,purchase_address_id, purchase_address, street,
							 city,state,zipcode, quantity, price_each,revenue)
SELECT order_dimension.order_id, product_dimension.product_id, product_dimension.product, order_date_dimension.order_date_id, 
		order_date_dimension.order_date, order_date_dimension.date, order_date_dimension."time",
		order_date_dimension.order_date_year, order_date_dimension.order_date_quarter,
		order_date_dimension.order_date_month, order_date_dimension.order_date_week, order_date_dimension.order_date_day,
		address_dimension.purchase_address_id, address_dimension.purchase_address,
		address_dimension.street, address_dimension.city, address_dimension.state, address_dimension.zipcode,
		sales_2019_transformed.quantity, sales_2019_transformed.price_each, SUM(price_each*quantity) AS revenue
		
FROM sales_2019_transformed
	INNER JOIN product_dimension ON sales_2019_transformed.product = product_dimension.product
	INNER JOIN order_date_dimension ON sales_2019_transformed.order_date = order_date_dimension.order_date
	INNER JOIN address_dimension ON sales_2019_transformed.purchase_address = address_dimension.purchase_address
	INNER JOIN order_dimension ON sales_2019_transformed.order_id = order_dimension.order_id
	
GROUP BY order_dimension.order_id, product_dimension.product_id, product_dimension.product, order_date_dimension.order_date_id, 
		order_date_dimension.order_date, order_date_dimension.date, order_date_dimension."time",
		order_date_dimension.order_date_year, order_date_dimension.order_date_quarter,
		order_date_dimension.order_date_month, order_date_dimension.order_date_week, order_date_dimension.order_date_day,
		address_dimension.purchase_address_id, address_dimension.purchase_address,
		address_dimension.street, address_dimension.city, address_dimension.state, address_dimension.zipcode,
		sales_2019_transformed.quantity, sales_2019_transformed.price_each
ORDER BY order_id ASC;

COPY sales_2019 TO 'C:\sampledb\albums.csv' DELIMITER ',' CSV HEADER;
