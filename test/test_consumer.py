import pytest
import os
import sys
import json
import time
import random
import requests
from dotenv import load_dotenv
from consumer import kafkaConsumer
from collections import OrderedDict
from kafka import KafkaConsumer, TopicPartition
from elasticsearch import Elasticsearch, helpers




load_dotenv()
c = kafkaConsumer()

urlGclid = os.getenv("URL_GCLID")
urlTRADB = os.getenv("URL_TRADB")
ElasticURL = os.getenv("ELATIC_URL")
hostPort = [os.getenv("HOST_PORT")]

def test_envUrl():
	assert len(ElasticURL) != 0, "the list is non empty"
	assert len(urlGclid) != 0, "the list is non empty"
	assert len(urlTRADB) != 0, "the list is non empty"
	assert len(hostPort) != 0, "the list is non empty"


def test_checkPhoneFormat():
	number = '12345678'
	assert c.checkPhoneFormat(number) == '+12345678'

def test_getPayloadExtractor():
	gclid = ['EAIaIQobChMI5qWssKDu7AIVBIzICh0olwHwEAAYASAAEgI03vD_BwE']
	assert bool(c.getPayloadExtractor(gclid, urlGclid)) == True

