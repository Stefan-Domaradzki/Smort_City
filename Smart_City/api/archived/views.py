from rest_framework.decorators import api_view
from rest_framework.response import Response
from ..models import RZTraffic
from django.db.models import Avg

@api_view(['GET'])
def avg_traffic_hour(request):
    data = (
        RZTraffic.objects.values('date_time', 'traffic_count')
        .annotate(avg_volume=Avg('traffic_count'))
        .order_by('date_time')
    )
    return Response(list(data))

@api_view(['GER'])
def test(request):
    return Response({"status": "ok", "message": "Tu będą dane archived"})
