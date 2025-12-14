from rest_framework.decorators import api_view
from rest_framework.response import Response
from ..models import RZTraffic
from ..models import RZWeather

from ..serializers import RZWeatherSerializer

from django.db.models import Avg

@api_view(['GET'])
def test(request):
    pass
    data = {'x':'1', 'y':'2'}
    return Response(list(data))

@api_view(['GET'])
def avg_traffic_hour(request):
    data = (
        RZTraffic.objects.values('date_time')
        .annotate(avg_volume=Avg('traffic_count'))
        .order_by('-date_time')[:24] 
    )

    return Response(data)
