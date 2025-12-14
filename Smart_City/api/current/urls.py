from django.urls import path
from .views import avg_traffic_hour

urlpatterns = [
    path('avg_traffic/', avg_traffic_hour),

]