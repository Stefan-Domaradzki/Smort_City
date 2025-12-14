import json

from rest_framework.response import Response
from rest_framework.decorators import api_view

from django.db import connection

from .models import RZTraffic
from .serializers import SensorSerializer

from .models import RZWeather
from .serializers import RZWeatherSerializer

@api_view(['GET'])
def system_health(request):
    return Response({"status": "ok", "message": "Django API działa poprawnie"})

@api_view(['GET'])
def sensors_list(request):
    sensors = (
        RZTraffic.objects
        .values('measuring_point', 'location')
        .distinct()
    )
    serializer = SensorSerializer(sensors, many=True)
    return Response(serializer.data)

@api_view(['GET'])
def weather_head(request):
    """
    Zwraca pierwsze 5 rekordów z tabeli rz_weather
    """
    queryset = RZWeather.objects.all().order_by('timestamp')[:5]
    serializer = RZWeatherSerializer(queryset, many=True)
    return Response(serializer.data)