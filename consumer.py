import os
import sys
import json
import time
import random
import requests
from dotenv import load_dotenv
from collections import OrderedDict
from kafka import KafkaConsumer, TopicPartition
from elasticsearch import Elasticsearch, helpers


load_dotenv()


class kafkaConsumer(object):
    
    def dataFromElastic(self, es, number):
        gclid_list =[]
        number = self.checkPhoneFormat(number) 

        data = es.search(index = 'call_rail_leads', body={
                "size": 1000,
                "query": {
                "bool": {
                  "must": [
                    {"term": {
                      "callernum.keyword": {
                        "value": number
                      }
                    }},
                    {"term": {
                      "call_type.keyword": {
                        "value": "post_call"
                      }
                    }}
                  ]
                }
              }
            })
        for i in range(0, len(data["hits"]["hits"])):
            gclid_list.append(data["hits"]["hits"][i]["_source"]['gclid'])

        gclids = list(OrderedDict.fromkeys(gclid_list))
        return gclids

    def checkPhoneFormat(self, number):
        if number[0]!='+':
            number = '+'+number
        return number


    def getPayloadExtractor(self, gclids, urlGclid):
        ResultGoogle={}
        for i in gclids:
            link = urlGclid+i
            r = requests.post(link)
            ResultGoogle['data'] = r.json()
        return ResultGoogle
    
    def correctUniCode(self, message):
        message = eval(json.dumps(message))
        return message
    

    def postTraDb(self, TRA_DB_LeadPayload, urlTRADB):
        r = requests.post(urlTRADB, data=TRA_DB_LeadPayload)
        print( r.json())    
    
    def listen_to_topic(self):
        urlGclid = os.getenv("URL_GCLID")
        urlTRADB = os.getenv("URL_TRADB")
        ElasticURL = os.getenv("ELATIC_URL")
        hostPort = [os.getenv("HOST_PORT")]

        while True:
            es = Elasticsearch([ElasticURL],timeout=60)


            consumer = KafkaConsumer("dev_inbound_leads", 
                         bootstrap_servers=hostPort, 
                         group_id="dev_inbound_leads_group",
                         auto_offset_reset="earliest",
                         enable_auto_commit=False,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                )

            print("started listening")
            for message in consumer:
                time.sleep(1.5)
                try:
                    consumer.commit()
                except Exception as e:
                    print(e)
                data = message.value
                try:
#                     data = correctUniCode(payload)
                    gclids = self.dataFromElastic(es, data['phone'])

                    payLoad = self.getPayloadExtractor(gclids, urlGclid)
                    TRA_DB_LeadPayload = {
                                            "leadId": data["lead"]["_id"],
                                            "gclId": payLoad['data']["payload"]["gclid"],
                                            "g_analytics": payLoad['data']["payload"]
                                         }

                    self.postTraDb(TRA_DB_LeadPayload, urlTRADB)
                except Exception as e:
                    print(e)