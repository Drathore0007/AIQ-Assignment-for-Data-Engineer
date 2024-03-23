
# AIQ Data pipeline project 


### Challenge Summary  

```
You are tasked with building a comprehensive sales data pipeline for a retail company. The pipeline 
should combine generated sales data with data from external sources, perform data 
transformations and aggregations, and store the final dataset in a database. The aim is to enable 
analysis and derive insights into customer behaviour and sales performance. 

```

### Solution Details:

This Git repo holds the script for Sala data ingestion, script are developed in python and ingesting the data in the PostgreSQL DB.


* Code: AIQ_APIs Codes (folder where all your code resides)
    * __init__.py
* setup.py (file required to create a egg package)
* requirement.txt (detail  of required module in this project)
* output Folder: this folder holds all csv and psdf file , both files are exported inthe end of the project.
* DataFiles: This folder contains sales csv file which is being used in the notebooks code.
* Power BI report: this folder has PBix file which was developed by sourcing data from postgresql db. 

when you create a new project with this script, you will get a folder with project name inside the path you have give with below file in it.


## Preinstallation required:
* Window or Ubuntu machine wiht jupyter notebook
* Python 3.7.x or above
* Postgresql 13 or above


### Step1: Set up Postgres DB
To set up the noteboo on your environment, you need to have a postgresql server and you must have a user who has create db, schema and tables permisions.

Once you have a database and user, we need to connect to database and run the the scrit given in  the 
```
.\AIQ project\code\SQL\createdb.sql
.\AIQ project\code\SQL\createtables.sql
```
[Create DB Script](/code/SQL/createdb.sql)

[Create Tables Script](/code/SQL/createtables.sql)

This can be done by serval IDEs like vscode, pgadmin and we can directly execute a notebook provided in the the report.
```
.\AIQ project\code\SetUpDB.ipynb
```
[Set-Up-DB notebook](/code/SetUpDB.ipynb)

Once we create the tables we will find the relation between dim_customers and Sales table based on Customer_ID, ID column, we have use this relationship in power bi reporting.

![alt text](./img/img1.PNG) 

Once Db and table have been created in the postgresql db, we can start the load of the data from the source.

### Step2: Running the Notebooks.

in this project we have three sources
* Sales csv file:  This file can be found in below location
  
  C:\Users\dharm\Desktop\Projects\AIQ project\DataFiles\AIQSalesData.csv

* User/Customer Data: this data is nested json data which can be found at given url https://jsonplaceholder.typicode.com/users 

* Weather Data: this is our third source of data which we will extract from Openweather source.
https://openweathermap.org/

to user this API we need to register on the side and get APIkey, which is free for limitted requests for per day.
![alt text](./img/img2.PNG) 

Once we get this APIkey, we need to put this key in our code.
Goto below location in the directory and replace the key with your Key.

.\AIQ project\code\AIQSales\Gettoken.py

![alt text](./img/img3.PNG) 

Note: i have places a samplekey, this key will not work for you.


# Step 2.A: Load dim_Customers
To load the data from dim_customers table, we will need to execute 

[Customer_Load](/code/Customer_Load.ipynb)

Execute this notebook in vscode or jupyter notebook. 
once notebook run successfully , we can validate the data in the postgres db.

### Note: Customer data load have been performed with upsert method. we have stage table to hold new customers details and all stage data gets into main dimenstion table by using SCD type-2 insertion.



# Step2.B: Load Sales data with weather details.
To load sales data with weather details we will use LoadSales.ipynb notebook.

all the logic have been built to bring weather details based on lat and long provided in the dim_customers table.

[Load Sales Table](/code/LoadSales.ipynb)

### Note: Sales data load have been performed with Delete-Insert method. we have stage table to hold incomming sales details and all stage data gets into main sales fact table by sing SCD type-1 insertion. This is performed due to some complexcity faced due to  sales order id having multiple rows in the sales flat file

# Step3: Power BI reporting for Data Analysis and Finsing Data Insights
Once the data ingestion have been completed, we can load this data into power bi desktop and create data model to create reports.

![alt text](./img/img5.PNG)

Power bi support postgres sql db connection, so we do not need to installor make any configurations.
All we will require is 
* server name (localhost)
* database name (AIG)
* user (postgres)
* password ()

### Date table addition.
as we did not load any date table in the system and we will require the dim_date table to perform data analysis on time dimention.

We have create a dim_date table in the power bi with M query.

``` m query
let
    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("i45WMjDUByIjAyMDpdhYAA==", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [StartDate = _t]),
    #"Added Custom" = Table.AddColumn(Source, "EndDate", each Date.From("2030-12-31")),
    #"Changed Type2" = Table.TransformColumnTypes(#"Added Custom",{{"EndDate", type date}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Changed Type2",{{"StartDate", type date}}),
    #"Added Custom1" = Table.AddColumn(#"Changed Type", "Dates", each {Number.From([StartDate])..Number.From([EndDate])}),
    #"Expanded Dates" = Table.ExpandListColumn(#"Added Custom1", "Dates"),
    #"Changed Type1" = Table.TransformColumnTypes(#"Expanded Dates",{{"Dates", type date}}),
    #"Removed Columns1" = Table.RemoveColumns(#"Changed Type1",{"StartDate", "EndDate"}),
    #"Added Custom2" = Table.AddColumn(#"Removed Columns1", "Year", each Date.Year([Dates])),
    #"Added Custom3" = Table.AddColumn(#"Added Custom2", "Month", each Date.Month([Dates])),
    #"Added Custom4" = Table.AddColumn(#"Added Custom3", "MonthName", each Date.MonthName([Dates])),
    #"Added Custom5" = Table.AddColumn(#"Added Custom4", "ShortMonthName", each Text.Start([MonthName],3)),
    #"Added Custom6" = Table.AddColumn(#"Added Custom5", "Quarter", each Date.QuarterOfYear([Dates])),
    #"Changed Type3" = Table.TransformColumnTypes(#"Added Custom6",{{"Quarter", type text}}),
    #"Added Custom7" = Table.AddColumn(#"Changed Type3", "QtrText", each "Qtr "& [Quarter])
in
    #"Added Custom7"

```

you can create a blankquery in power bi and past this m query to create a date table.

## Create model relationship
after power bi modeling part, we will create a relationship between sales, dim_customers and dim_date table.

![alt text](./img/img1.PNG) 

Relationship between tables will be as below:
* dim_customer(ID) > sales(customer_ID) (one-to-many relationship)
* dim_date(date) > sales(order_date) (one-to-many relationship)

Once the relationship have been completed, we will create required measures to perform the analysis.

Note: we have marked dim_date table as date table in power bi, so that we can utilize time inteligence functions.

![alt text](./img/img6.PNG)

To analyze sales data we have create power bi measures by using dax functions.
Dax function are used to create Net sales, YTm, MTD, QTD and PYTD( previoous year to date) sales, sales variance, variance % and etc.
This all DAX formula can be found in the power bi report mentioned below.

Power bi Report PDF Sample: [AIQ Sales Analysis](/outputfiles/AIQSalesAnalysis.pdf)

Power BI report details: [AIQ Sales Analysis](/outputfiles/AIQSalesAnalysis.htm)


## Results:
After everythin get completed we can see that all notebook are running as weexpected and this can be schedule with help of any cloud tools like
Azure Synapse, Azure ADF, Azure Databricks, AWS glue, Airflow and etc.


Project Final output we can see in the power Bi Report, where we have performed the analysis.
This report can be shared with business and we can publish this report to power bi online services where report will be refresh on a schedule time.

## AIQ Sales Analysis by Time
![alt text](./img/img7.PNG)

## Sales With Customer, City and Products

![alt text](./img/img8.PNG)

## AI Analysts of Sales With Weather

![alt text](./img/img9.PNG)



# Extra Step to create Egg and whl file for moving the code to cloud.

#### To Create EGG file from package
* To create your app or to create EGG file use below command in project folder.  
  $ *python setup.py bdist_egg* 

you need to be in you AIQ Project folder directory to build your app with python. this command will create a folder name Dist in you project directory and egg_file_package will be inside that folder.


#### Submiting application
pip wheel --wheel-dir=wheels -r requirements.txt

# Download all the wheel file of packages:
pip download -r requirements.txt -d C:\Users\dharm\Desktop\LDH_Development\Codes\env\Package

# Download package from specfic location
pip install -r requirements.txt --no-index --find-links C:\Users\dharm\Desktop\LDH_Development\Codes\env\Package

pip install -r requirements.txt  ./LDHPackage-1.0.4.tar.gz

# Install all package in spefic dir:
pip install --prefix=C:\Users\dharm\Desktop\LDH_Development\Codes\env\Package -r requirements.txt 

