from django.shortcuts import render
from django.http import HttpResponse, JsonResponse

import json

from rest_framework.response import Response
from rest_framework.decorators import api_view

from .sensors_consumer import JunctionConsumer
from kafka import KafkaConsumer

@api_view(['GET'])
def index(request):
    return HttpResponse('<h1> Hello SmartCity! </h1>')

@api_view(['GET'])
def test_flutter_connection(request):
    return JsonResponse({'message': 'Hello from Django'})

@api_view(['GET'])
def sensors_status(request):

    kafka_consumer = KafkaConsumer(
        'heartbeat',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        group_id=None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        )

    status_consumer = JunctionConsumer(kafka_consumer,"test_heartbeat")
    sensors_states = status_consumer.read_last_heartbeat_per_sensor()

    kafka_consumer.close()

    return Response(sensors_states)

