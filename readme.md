
# AIQ Data pipeline project 


### Challenge Summary  

```
You are tasked with building a comprehensive sales data pipeline for a retail company. The pipeline 
should combine generated sales data with data from external sources, perform data 
transformations and aggregations, and store the final dataset in a database. The aim is to enable 
analysis and derive insights into customer behaviour and sales performance. 

```

### Solution Details:

This Git repo holds the script for Sala data ingestion, The script is developed in Python and ingests the data in the PostgreSQL DB.


* Code: AIQ_APIs Codes (the folder where all your code resides)
    * __init__.py
* setup.py (file required to create an egg package)
* requirement.txt (detail  of required module in this project)
* output Folder: this folder holds all csv and pdf files, both files are exported at the end of the project.
* DataFiles: This folder contains sales csv file which is being used in the notebook's code.
* Power BI report: this folder has a PBix file which was developed by sourcing data from Postgresql db. 

when you create a new project with this script, you will get a folder with the project name inside the path you have given with the below file in it.


## Preinstallation required:
* Window or Ubuntu machine with jupyter notebook
* Python 3.7.x or above
* Postgresql 13 or above


### Step1: Set up Postgres DB
To set up the notebook on your environment, you need to have a Postgresql server and you must have a user who has created db, schema and tables permissions.

Once you have a database and user, we need to connect to the database and run the script given in  the 
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

Once we create the tables we will find the relation between dim_customers and the Sales table based on the Customer_ID, ID column, we have used this relationship in Power BI reports.

![alt text](./img/img1.PNG) 

Once Db and table have been created in the Postgresql db, we can start the load of the data from the source.

### Step 2: Running the Notebooks.

in this project, we have three sources
* Sales CSV file:  This file can be found in the below location
  
  C:\Users\dharm\Desktop\Projects\AIQ project\DataFiles\AIQSalesData.csv

* User/Customer Data: this data is nested JSON data which can be found at the given URL https://jsonplaceholder.typicode.com/users 

* Weather Data: this is our third source of data which we will extract from the Openweather source.
https://openweathermap.org/

to use this API we need to register on the side and get an API key, which is free for limited requests for per day.
![alt text](./img/img2.PNG) 

Once we get this API key, we need to put this key in our code.
Go to the below location in the directory and replace the key with your Key.

.\AIQ project\code\AIQSales\Gettoken.py

![alt text](./img/mg3.PNG) 

Note: I have placed a sample key, this key will not work for you.


# Step 2.A: Load dim_Customers
To load the data from the dim_customers table, we will need to execute 

[Customer_Load](/code/Customer_Load.ipynb)

Execute this notebook in vscode or jupyter notebook. 
once the notebook runs successfully, we can validate the data in the Postgres DB.

### Note: Customer data load have been performed with the upsert method. we have a stage table to hold new customers' details and all stage data gets into the main dimension table by using SCD type-2 insertion.



# Step2.B: Load Sales data with weather details.
To load sales data with weather details we will use LoadSales.ipynb notebook.

all the logic has been built to bring weather details based on lat and long provided in the dim_customers table.

[Load Sales Table](/code/LoadSales.ipynb)

### Note: Sales data loads have been performed with the Delete-Insert method. we have a stage table to hold incoming sales details and all stage data gets into the main sales fact table by sing SCD type-1 insertion. This is performed due to some difficulties faced due to  the sales order ID having multiple rows in the sales flat file

# Step3: Power BI reporting for Data Analysis and Finding Data Insights
Once the data ingestion has been completed, we can load this data into the Power BI desktop and create a data model to create reports.

![alt text](./img/img5.PNG)

Power bi supports Postgres SQL DB connection, so we do not need to install or make any configurations.
All we will require is 
* server name (localhost)
* database name (AIG)
* user (postgres)
* Password ()

### Date table addition.
as we did not load any date table in the system and we will require the dim_date table to perform data analysis on time dimension.

We have created a dim_date table in the power BI with M query.

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

you can create a blank query in Power BI and paste this m query to create a date table.

## Create a model relationship
after the power bi modelling part, we will create a relationship between sales, dim_customers and dim_date table.

![alt text](./img/img1.PNG) 

The relationship between tables will be as below:
* dim_customer(ID) > sales(customer_ID) (one-to-many relationship)
* dim_date(date) > sales(order_date) (one-to-many relationship)

Once the relationship has been completed, we will create the required measures to perform the analysis.

Note: we have marked the dim_date table as a date table in Power BI, so that we can utilize time intelligence functions.

![alt text](./img/img6.PNG)

To analyze sales data we have created Power BI measures by using Dax functions.
Dax functions are used to create Net sales, YTm, MTD, QTD and PYTD(previous year to date) sales, sales variance, variance % and etc.
These DAX formulas can be found in the power BI report mentioned below.

Power bi Report PDF Sample: [AIQ Sales Analysis](/outputfiles/AIQSalesAnalysis.pdf)

Power BI report details: [AIQ Sales Analysis](/outputfiles/AIQSalesAnalysis.htm)


## Results:
After everything get completed we can see that all notebooks are running as expected and this can be scheduled with help of any cloud tools like
Azure Synapse, Azure ADF, Azure Databricks, AWS glue, Airflow and etc.


The project's Final output we can see in the power BI Report, where we have performed the analysis.
This report can be shared with businesses and we can publish this report to power BI online services where the report will be refreshed at a scheduled time.

## AIQ Sales Analysis by Time
![alt text](./img/img7.PNG)

## Sales With Customer, City and Products

![alt text](./img/img8.PNG)

## AI Analysts of Sales With Weather

![alt text](./img/img9.PNG)



# Extra Step to create Egg and WHL file for moving the code to the cloud.

#### To Create an EGG file from the package
* To create your app or to create an EGG file use the below command in the project folder.  
  $ *python setup.py bdist_egg* 

you need to be in your AIQ Project folder directory to build your app with Python. this command will create a folder named Dist in your project directory and egg_file_package will be inside that folder.


#### Submitting application
pip wheel --wheel-dir=wheels -r requirements.txt

# Download all the wheel files of packages:
pip download -r requirements.txt -d .\Codes\env\Package

# Download package from specific location
pip install -r requirements.txt --no-index --find-links .\Codes\env\Package

pip install -r requirements.txt  

# Install all packages in spefic dir:
pip install --prefix=C:\Users\dharm\Desktop\LDH_Development\Codes\env\Package -r requirements.txt 

