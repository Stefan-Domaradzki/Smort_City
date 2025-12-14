from django.urls import path, include
from . import views


#     path("system/health/", views.system_health, name='system-health'),
urlpatterns = [
    path('alerts/', include('api.alerts.urls')),
    path('archived/', include('api.archived.urls')),
    path('current/', include('api.current.urls')),
    path('predict/', include('api.predictions.urls')),
    path('sensors/', views.sensors_list, name='sensors-list'),
    path('weather/head/', views.weather_head, name='weather-head'),
]
