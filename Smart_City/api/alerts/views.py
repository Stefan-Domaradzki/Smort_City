from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

import datetime;
from ..models import alerts_and_news

@api_view(['GET'])
def fetch_recent_alerts(request):
    data = (
        alerts_and_news.objects
        .values('date_time', 'street_name', 'alert_type', 'alert_description')
        .order_by('-date_time')[:10] 
    )

    return Response(data)

@api_view(['POST'])
def register_alert(request):
    required_fields = ["street_name", "alert_type"]

    for field in required_fields:
        if field not in request.data:
            return Response(
                {"error": f"Missing field: {field}"},
                status=status.HTTP_400_BAD_REQUEST
            )

    street_name = request.data["street_name"]
    alert_type = request.data["alert_type"]
    alert_description = request.data.get("alert_description", "")

    alerts_and_news.objects.create(
        date_time=datetime.datetime.now(),
        street_name=street_name,
        alert_type=alert_type,
        alert_description=alert_description,
    )

    return Response(
        {
            "status": "success",
            "message": "Alert registered",
            "data": {
                "street_name": street_name,
                "alert_type": alert_type,
                "alert_description": alert_description
            }
        },
        status=status.HTTP_201_CREATED
    )