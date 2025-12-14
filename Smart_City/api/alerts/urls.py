from django.urls import path
from .views import fetch_recent_alerts, register_alert

urlpatterns = [
    path('recent_alerts/', fetch_recent_alerts),
    path('register_alert/', register_alert),
]