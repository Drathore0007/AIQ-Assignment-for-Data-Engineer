
drop table IF  EXISTS Dim_Customers_stg ; 

CREATE TABLE IF NOT EXISTS Dim_Customers_stg(
	id integer ,
	name character varying(30),
	username character varying(30),
	email character varying(30),
	phone character varying(50) NULL,
	website character varying(50) NULL,
	street character varying(100) NULL,
	city character varying(30),
	zipcode character varying(30),
	suite character varying(30),
	lat character varying(30) NULL,
	lng character varying(30) NULL,
	companyname character varying(50) NULL,
	catchPhrase character varying(100) NULL,
	bs character varying(100) NULL,
	inserted_date  date NOT NULL,
	inserted_by character varying(10) NULL,
	updated_date  date NOT NULL,
	updated_by character varying(10) NULL
) ;

drop table IF  EXISTS Dim_Customers ; 

CREATE TABLE IF NOT EXISTS Dim_Customers(
	id integer ,
	name character varying(30),
	username character varying(30),
	email character varying(30),
	phone character varying(50) NULL,
	website character varying(50) NULL,
	street character varying(100) NULL,
	city character varying(30),
	zipcode character varying(30),
	suite character varying(30),
	lat character varying(30) NULL,
	lng character varying(30) NULL,
	companyname character varying(50) NULL,
	catchPhrase character varying(100) NULL,
	bs character varying(100) NULL,
	inserted_date  date NOT NULL,
	inserted_by character varying(10) NULL,
	updated_date  date NOT NULL,
	updated_by character varying(10) NULL
) ;


drop table IF EXISTS sales;



CREATE TABLE sales(
	order_id integer not NULL,
	customer_id integer not NULL,
	product_id integer not NULL,
	quantity integer not NULL,
	price DECIMAL(15,2) not NULL,
	order_date date NOT NULL,
    id integer not NULL,
    lat character varying(30) NULL, 
    lng character varying(30) NULL,
    weather character varying(30) NULL,
    description character varying(30) NULL,
    temp DECIMAL(15,2) not NULL,
    pressure DECIMAL(15,2) not NULL,
    humidity DECIMAL(15,2) not NULL, 
    grnd_level DECIMAL(15,2) not NULL,
    sea_level DECIMAL(15,2) not NULL,
    visibility character varying(30) NULL,
    wind_speed DECIMAL(15,2) not NULL,
    wind_deg DECIMAL(15,2) not NULL,
    wind_gust DECIMAL(15,2) not NULL,
    inserted_date date NOT NULL, 
    inserted_by character varying(10) NULL,
    updated_date date NOT NULL, 
    updated_by character varying(10) NULL
) ;


drop table IF  EXISTS sales_stg

CREATE TABLE  IF NOT EXISTS sales_stg(
	order_id integer not NULL,
	customer_id integer not NULL,
	product_id integer not NULL,
	quantity integer not NULL,
	price DECIMAL(15,2) not NULL,
	order_date date NOT NULL,
    id integer not NULL,
    lat character varying(30) NULL, 
    lng character varying(30) NULL,
    weather character varying(30) NULL,
    description character varying(30) NULL,
    temp DECIMAL(15,2) not NULL,
    pressure DECIMAL(15,2) not NULL,
    humidity DECIMAL(15,2) not NULL, 
    grnd_level DECIMAL(15,2) not NULL,
    sea_level DECIMAL(15,2) not NULL,
    visibility character varying(30) NULL,
    wind_speed DECIMAL(15,2) not NULL,
    wind_deg DECIMAL(15,2) not NULL,
    wind_gust DECIMAL(15,2) not NULL,
    inserted_date date NOT NULL, 
    inserted_by character varying(10) NULL,
    updated_date date NOT NULL, 
    updated_by character varying(10) NULL
) ;




ALTER TABLE Dim_Customers ADD CONSTRAINT pk_Dim_Customers
  PRIMARY KEY (id);

/*
ALTER TABLE sales ADD CONSTRAINT pk_sales
  PRIMARY KEY (order_id);
ALTER TABLE sales ADD CONSTRAINT fk_sales
  FOREIGN KEY (customer_id) REFERENCES Dim_Customers(id);
*/

# check the tables

Select *  from Dim_Customers_stg ; 
select * from Dim_Customers


Select *  from sales_stg ; 
Select *  from sales ; 