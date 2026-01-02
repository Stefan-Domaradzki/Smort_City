from rest_framework import serializers
from .models import SensorsStates


class SensorStatesSerializer(serializers.ModelSerializer):
    class Meta:
        model = SensorsStates
        fields = "__all__"