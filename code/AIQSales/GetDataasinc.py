import requests 
import json
import aiohttp
import asyncio
from pprint import pprint
status= 200
cnt= 1

class AIQ_asincGetWeather:
    
    def __init__(self, APIToken, APIurl, baseUrl):

        self.APIToken = APIToken
        self.APIurl =  APIurl #
        self.baseUrl = baseUrl #
        
    async def fetch_all(self, session, number_of_requests, concurrent_limit):
        tasks = []
        global status
        global cnt

        sem = asyncio.Semaphore(concurrent_limit)
        
        async def fetch(session, i):

            global status
            global cnt
            offset= i
            limit= '100'
            
            lastLimit= offset+100
            
            APIFullurl = "{0}/{1}?business={2}&country={3}&modifiedFrom={4}&offset={5}&limit={6}".\
            format( self.APIurl , self.baseUrl, self.business, self.country, self.modifiedFrom, offset, limit)
            
            await asyncio.sleep(1)
            async with session.get(APIFullurl,  headers={'Authorization': 'Bearer ' + self.APIToken} ) as response: 
                #customerdata =  await response.text()
                customerdata =  await response.text()

                try:
                    status = response.status
                    cnt =  len(customerdata)
                    customerdata = json.loads(customerdata)
                except ValueError:
                    pass
                
            sem.release()
            return customerdata

        for i in range(0, self.recordLimit, 100):

            if((cnt>0)&(status==200)):
                await sem.acquire()
                print("running for offeset ", i)
                task = asyncio.ensure_future(fetch(session, i))
                tasks.append(task)
            else:
                print("breaking the code")
                break
        results = await asyncio.gather(*tasks)
        return results

    async def main(self, number_of_requests, concurrent_limit):
        async with aiohttp.ClientSession() as session:
            result = await self.fetch_all(session, number_of_requests, concurrent_limit)
        
        return result


    def Getdata(self, number_of_requests,concurrent_limit):
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(self.main(number_of_requests, concurrent_limit))

        print(len(results))
        loop.stop()
        return results
        
    if __name__ == '__main__':
        print("started calling API")
        
        number_of_requests = 10
        concurrent_limit = 4
        
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(main(number_of_requests, concurrent_limit))
        print(len(results))
        pprint.pprint(results)
        loop.stop()




