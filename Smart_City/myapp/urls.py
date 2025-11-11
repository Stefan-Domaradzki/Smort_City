from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('', views.test_flutter_connection, name='test_flutter_connection'),
]
