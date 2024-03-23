
from pprint import pprint
from LDH_API.Gettoken import LDH_GetAuth 
from LDH_API.GetData import LDH_GetData
from LDH_API.LoadToDB import To_postgres
import pandas as pd

from datetime import datetime


# API Conenction Data
access_token_url="https://api-gw-dev.boehringer-ingelheim.com:8443/api/oauth/token"
client_id = '15aee13f-8bcb-47e8-b77e-e446f8ab175c'
client_secret = '1a897726-3d3e-49d7-942d-780dac43a291'

# Get APi access Token
token =  LDH_GetAuth(access_token_url, client_id, client_secret)
access_token=  token.access_token

print(access_token)

# create and object to get data
customer =  LDH_GetData(access_token)

# API Paramters
customerapi= 'financial-customer-data-api/customers'
business= 'ah'
country= 'us'
modifiedFrom= '2019-06-02T14:00:00-05:00'
modifiedTo= '2020-06-02T14:00:00-05:00'
offset= '100'
limit= '100'

BICustomers =customer.get_Customers(customerapi, business, country, modifiedFrom, modifiedTo, offset, limit )
print('\n  Customer Location Data: ' )
#pprint(BICustomers)

print("Loading data to postgres")


pg_local= {
    "host"      : "172.16.10.146",
    "database"  : "ldh",
    "user"      : "postgres",
    "password"  : "root"
}

#create a connection to Databases
conn = To_postgres(pg_local)

cust_list = []
for customer in BICustomers:
    cust= {}
    if len(customer)>0:
        try:
            
            cust['location_id']= customer['shipTo']['id']
            cust['location_status']= customer['shipTo']['status']
            cust['location_type']= customer['shipTo']['type']
            cust['price_list']= customer['shipTo']['salesPriceListId']
            cust['location_name']= customer['shipTo']['name']
            cust['ship_to_address_1']= customer['shipTo']['addressLine1']
            if 'addressLine2' in customer['shipTo']: cust['ship_to_address_2'] = customer['shipTo']['addressLine2']
            else: cust['ship_to_address_2'] = ''
            cust['ship_to_postal']= customer['shipTo']['zipOrPostalCode']
            cust['ship_to_city']= customer['shipTo']['city']
            cust['ship_to_state']= customer['shipTo']['stateCode']
            cust['ship_to_country']= customer['shipTo']['countryName']
            cust['customer_class']= customer['shipTo']['customerClass']
            cust['buying_group']= '' #customer['shipTo']['buying_group']
            cust['self_assigned_identifier']= '' #customer['shipTo']['self_assigned_identifier']
            
            if 'id' in customer['billTo']: cust['bill_to_location_id'] = customer['billTo']['id']
            else: cust['bill_to_location_id'] = ''
                
            cust['ship_to_location_id']= customer['shipTo']['id']
            cust['sold_to_location_id']= customer['soldTo']['id']
            
            if 'id' in customer['billTo']: cust['payer_location_id'] = customer['billTo']['id']
            else: cust['payer_location_id'] = ''
            
            cust['inserted_date']=datetime.now().date()
            cust['inserted_by']= 'app_ldh_rw'
            cust['updated_date']=datetime.now().date()
            cust['updated_by']='app_ldh_rw'

                
        except KeyError as e:
            print( "Error Message: location %s missing field %s" % (customer['shipTo']['id'], e))
            
        
    cust_list.append(cust)
    

customer_df = pd.DataFrame(cust_list)
stgTable='stage_customers'
Table='customers'
keycol= 'location_id'

#creates customer table is not exists in the destination
#conn.CreateCustomer()

#trigger upsert method
conn.UpsertData(customer_df, stgTable, Table, keycol)

