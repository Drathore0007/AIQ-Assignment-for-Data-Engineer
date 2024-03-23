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
rebates =  LDH_GetData(access_token)

#Call Customer rebates API
rebateapi= 'rebates-data-api/paid-rebates'
location= '1165758'
business= 'ah'
country= 'us'

CustomerRebateData =  rebates.get_rebate(rebateapi, location, business, country)

print('\n  Customer rebate Data: ' )
#pprint(CustomerRebateData)

#Column Mapping of API field with database table

CustomerRebate_list= []

rebatedetails= []
detail_L= []


rebates =  [CustomerRebateData]
for rebate in rebates:
    customerrebatesDetails ={}
    
    customerrebatesDetails['customer_bill_to_id']= location
    customerrebatesDetails['rebates_paid_prior_year']= rebate['paidPriorYear']
    customerrebatesDetails['rebates_paid_ytd']= rebate['paidYearToDate']
    customerrebatesDetails['currency_code']= 'USD'
    customerrebatesDetails['inserted_date']=datetime.now().date()
    customerrebatesDetails['inserted_by']= 'app_ldh_rw'
    customerrebatesDetails['updated_date']=datetime.now().date()
    customerrebatesDetails['updated_by']='app_ldh_rw'
    
    
    print(len(rebate['rebates']))
    for r in rebate['rebates']: 
        #print(r)
        rebate_D = {}
        rebate_D['customer_bill_to_id']= location
        rebate_D['erp_ref_id']= r['id']
        rebate_D['promotion_Name']= r['promotionName']
        rebate_D['payment_type']= r['paymentType']
        rebate_D['issue_date']= r['issueDate']
        rebate_D['rebate_payment_status']= r['paymentStatus']
        rebate_D['currency_iso3code']= r['currencyIso3Code']
        if 'updateDate' in r: rebate_D['status_date']= r['updateDate']
        else: rebate_D['status_date']= '1999-01-01'
        rebate_D['inserted_date']=datetime.now().date()
        rebate_D['inserted_by']= 'app_ldh_rw'
        rebate_D['updated_date']=datetime.now().date()
        rebate_D['updated_by']='app_ldh_rw'

      
        #rebate_D['details']= list(r['details'])
        
        for detail in r['details']:
            detail_D= {}
            detail_D['customer_bill_to_id']= location
            detail_D['erp_ref_id']= r['id']
            detail_D['rebate_amount']=  detail['amount']
            detail_D['description']=  detail['description']
            
            detail_L.append(detail_D)
        
        rebatedetails.append(rebate_D)
    
    CustomerRebate_list.append(customerrebatesDetails)

# Getting pandas dataframe

rebatedetails_df =  pd.DataFrame(rebatedetails)

detail_df =  pd.DataFrame(detail_L)

rebatedetails_df =  pd.merge(rebatedetails_df, detail_df, how='inner', on=['customer_bill_to_id', 'erp_ref_id'])

rebates = pd.DataFrame(CustomerRebate_list)

#print(CustomerRebate_df.head(10))



# postgres Connection parametrs
LOCAL_POSTGRES = {
    "host"      : "172.16.10.146",
    "database"  : "ldh",
    "user"      : "postgres",
    "password"  : "root"
}

LOCHAN_POST = {
    "host"      : "database-1.cdxepunr1l2g.us-west-2.rds.amazonaws.com",
    "database"  : "loyalty_data_hub",
    "user"      : "postgres",
    "password"  : "admin123"
}

PARTH_POSTGRES = {
    "host"      : "gogreen.clnmsfh0dazd.us-east-2.rds.amazonaws.com",
    "database"  : "loyalty_data_hub",
    "user"      : "postgres",
    "password"  : "Smarterp123"
}

#create a connection to Databases
conn = To_postgres(PARTH_POSTGRES)
 
#Creating dataframe for rebate table

paid_rebates = rebatedetails_df[['erp_ref_id', 'promotion_Name','rebate_amount','payment_type',
       'issue_date', 'rebate_payment_status', 'currency_iso3code',
       'status_date','inserted_date', 'inserted_by', 'updated_date',
       'updated_by']]

# to avoid Nan value from column
paid_rebates['status_date']=paid_rebates['status_date'].fillna(value ='1999-01-01')

# to remove duplicate value
paid_rebates= paid_rebates.drop_duplicates()

# to sum rebate amount to avoid duplicate

paid_rebates =paid_rebates.groupby(by=['erp_ref_id','promotion_Name','payment_type',
       'issue_date', 'rebate_payment_status', 'currency_iso3code',
       'status_date', 'inserted_date', 'inserted_by', 'updated_date',
       'updated_by']).sum().reset_index()

# Arrange Column in correct order
paid_rebates =paid_rebates[['erp_ref_id', 'promotion_Name','rebate_amount', 'payment_type', 'issue_date',
       'rebate_payment_status', 'currency_iso3code', 'status_date',
       'inserted_date', 'inserted_by', 'updated_date', 'updated_by'
       ]]       


paid_rebates_details = rebatedetails_df[['erp_ref_id',
     'description','rebate_amount',
     'inserted_date', 'inserted_by']]       

paid_rebates_details['rebate_details_desciption']= paid_rebates_details['description']

paid_rebates_details['rebate_details_amount']= paid_rebates_details['rebate_amount']

paid_rebates_details =  paid_rebates_details [['erp_ref_id',
     'rebate_details_desciption','rebate_details_amount',
     'inserted_date', 'inserted_by'] ]


stgTable1='stage_rebates'
Table1='rebates'
keycol1= 'customer_bill_to_id'

#trigger upsert method
conn.UpsertData(rebates, stgTable1, Table1, keycol1)       

stgTable2='stage_paid_rebates'
Table2='paid_rebates'
keycol2= 'erp_ref_id'

#trigger upsert method
conn.UpsertData(paid_rebates, stgTable2, Table2, keycol2)     


stgTable3='stage_paid_rebates_details'
Table3='paid_rebates_details'
keycol3= 'erp_ref_details_id'

#trigger upsert method
conn.deleteInsert(paid_rebates_details, stgTable3, Table3, keycol3)     
