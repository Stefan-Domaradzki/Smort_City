from rest_framework import serializers
from .models import RZTraffic
from .models import RZWeather

class SensorSerializer(serializers.ModelSerializer):
    class Meta:
        model = RZTraffic
        fields = ['measuring_point', 'location']

class RZWeatherSerializer(serializers.ModelSerializer):
    class Meta:
        model = RZWeather
        fields = "__all__"