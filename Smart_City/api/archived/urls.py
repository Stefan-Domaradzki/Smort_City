from django.urls import path
from .views import avg_traffic_hour, avg_traffic_all, traffic_by_weather_all, test

urlpatterns = [
    path('DO-NOT-USE-recent-traffic', avg_traffic_hour),
    path('avg-traffic', avg_traffic_all),
    path('traffic-by-weather', traffic_by_weather_all),
    path('test', test)
]