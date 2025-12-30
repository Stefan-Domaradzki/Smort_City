from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('test', views.test_flutter_connection, name='test_flutter_connection'),
    path('system-health/sensors-state', views.sensors_status, name='sensors_status'),
]
