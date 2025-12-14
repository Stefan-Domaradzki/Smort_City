from django.urls import path
from .views import predict_traffic

urlpatterns = [
    path('predict/', predict_traffic),

]