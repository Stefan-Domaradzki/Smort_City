from django.shortcuts import render
from django.http import HttpResponse, JsonResponse

import json

from rest_framework.response import Response
from rest_framework.decorators import api_view

from .models import SensorsStates
from .serializers import SensorStatesSerializer

#from .sensors_consumer import JunctionConsumer
#from kafka import KafkaConsumer

@api_view(['GET'])
def index(request):
    return HttpResponse('<h1> Hello SmartCity! </h1>')

@api_view(['GET'])
def test_flutter_connection(request):
    return JsonResponse({'message': 'Hello from Django'})

from django.db import connection

@api_view(['GET'])
def sensors_status(request):

    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT measuring_point,
                   sensor_state,
                   last_registered_malfunction,
                   last_update
            FROM sensors_states
            ORDER BY last_update DESC;
        """)
        rows = cursor.fetchall()

    data = [
        {
            "measuring_point": row[0],
            "sensor_state": row[1],
            "last_registered_malfunction": row[2],
            "last_update": row[3].isoformat(),
        }
        for row in rows
    ]

    return Response(data)
