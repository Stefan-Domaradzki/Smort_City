from rest_framework.decorators import api_view
from rest_framework.response import Response
from ..models import RZTraffic
from django.db.models import Avg
from django.db import connection

@api_view(['GET'])
def avg_traffic_hour(request):
    data = (
        RZTraffic.objects.values('date_time', 'traffic_count')
        .annotate(avg_volume=Avg('traffic_count'))
        .order_by('date_time')
    )
    return Response(list(data))

@api_view(['GET'])
def avg_traffic_all(request):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                weekday,
                hour,
                avg_traffic
            FROM mv_traffic_avg_by_weekday_hour
            ORDER BY weekday, hour
        """)

        rows = cursor.fetchall()

    data = [
        {
            "weekday": int(row[0]),
            "hour": int(row[1]),
            "avg_traffic": float(row[2]),
        }
        for row in rows
    ]

    return Response(data)

@api_view(['GET'])
def traffic_by_weather_all(request):
    with connection.cursor() as cursor:
        cursor.execute("""
            SELECT
                weekday,
                hour,
                precipitation_type,
                avg_traffic
            FROM mv_traffic_avg_by_precipitation
            ORDER BY weekday, hour, precipitation_type;
        """)
        rows = cursor.fetchall()

    data = [
        {
            "weekday": r[0],
            "hour": r[1],
            "precipitation": r[2],
            "avg_traffic": r[3],
        }
        for r in rows
    ]

    return Response(data)

@api_view(['GET'])
def test(request):
    return Response({"status": "ok", "message": "Tu będą dane archived"})
