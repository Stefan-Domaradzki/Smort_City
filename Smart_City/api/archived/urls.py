from django.urls import path
from .views import avg_traffic_hour, test

urlpatterns = [
    path('avg-traffic', avg_traffic_hour),
    path('test', test)
]