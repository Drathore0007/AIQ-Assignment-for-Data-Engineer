import requests 
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

class AIQ_GetData:
    def __init__(self, APIToken ):
        self.APIToken = APIToken

    def get_sales(self, location):
        salesLocation=  location
        my_file = Path(salesLocation)
        if my_file.is_file():
            Sales_df = pd.read_csv(salesLocation)
            return Sales_df
        else:
            message = "Either the file is missing or not readable"
            print(message)
            return message
        
    def get_Customers(self, location):
        customerurl=  location
        customer =  requests.get(customerurl, headers={'Authorization': 'Bearer ' + self.APIToken}, verify=True)
        if ( customer.ok and customer.status_code==200):
            customers = customer.json()
            cust_list = []
            for customer in customers:
                cust= {}
                if len(customer)>0:
                    try:
                        
                        cust['id']= customer['id']
                        cust['name']= customer['name']
                        cust['username']= customer['username']
                        cust['email']= customer['email']
                        cust['phone']= customer['phone']
                        cust['website']= customer['website']
                
                        cust['street']= customer['address']['street']
                        cust['city']= customer['address']['city']
                        cust['zipcode']= customer['address']['zipcode']
                        cust['suite']= customer['address']['suite']
                        cust['lat']= customer['address']['geo']['lat']
                        cust['lng']= customer['address']['geo']['lng']
                        
                        cust['companyname']= customer['company']['name']
                        cust['catchPhrase']= customer['company']['catchPhrase']
                        cust['bs']= customer['company']['bs']
                        
                        cust['inserted_date']=datetime.now().date()
                        cust['inserted_by']= 'Dharm'
                        cust['updated_date']=datetime.now().date()
                        cust['updated_by']='dharm'

                    except KeyError as e:
                        print( "Error Message: location %s missing field %s" % (customer['shipTo']['id'], e))
                    cust_list.append(cust)
            
            customer_df = pd.DataFrame(cust_list)

            return customer_df
        else:
            r=  customer
            message =  "HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text)
            print(message)
            return message
        
    def get_Weather(self, baseurl):
        weatherurl=  baseurl
        weather =  requests.get(weatherurl, headers={'Authorization': 'Bearer ' + self.APIToken}, verify=True)
        if ( weather.ok and weather.status_code==200):
            weather = weather.json()
            return weather
        else:
            r=  weather
            message =  "HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text)
            print(message)
            return message


    def get_WeatherData(self, locationlist):
    
        weather = []
        for  location in locationlist:
            print(location)
            weatherurl = "https://api.openweathermap.org/data/2.5/weather?lat={0}&lon={1}&appid={2}".\
            format(location[1], location[2], self.APIToken)
            
            weathers =  requests.get(weatherurl, headers={'Authorization': 'Bearer '}, verify=True)
            if ( weathers.ok and weathers.status_code==200):
                we = weathers.json()
                
            else:
                r=  weathers
                message =  "HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text)
                print(message)
                break
            
            weth= {}
            if len(we)>0:
                try:
                    weth['id'] = location[0]
                    weth['lat']= we['coord']['lat']
                    weth['lng']= we['coord']['lon']
                    weth['weather']= we['weather'][0]['main']
                    weth['description']= we['weather'][0]['description'] 
                    weth['temp']= we['main']['temp']
                    weth['pressure']= we['main']['pressure']
                    weth['humidity']= we['main']['humidity']
                    weth['grnd_level']= we['main']['grnd_level']
                    weth['sea_level']= we['main']['sea_level']
                    if 'visibility' in we['main']: weth['visibility']= we['main']['visibility']
                    else: weth['visibility']= ''

                    weth['wind_speed']= we['wind']['speed']
                    weth['wind_deg']= we['wind']['deg']
                    weth['wind_gust']= we['wind']['gust']
                    weth['pressure']= we['main']['pressure']
                    weth['inserted_date']= datetime.now()
                    weth['inserted_by']= 'Dharm'
                    weth['updated_date']= datetime.now()
                    weth['updated_by']='dharm'

                except KeyError as e:
                    print( "Error Message: location %s missing field %s" , e)

            weather.append(weth)

        Weather_df = pd.DataFrame(weather)

        return Weather_df
        
if __name__ == "__main__":
    pass

