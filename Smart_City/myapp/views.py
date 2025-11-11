from django.shortcuts import render
from django.http import HttpResponse, JsonResponse

# Create your views here.
def index(request):
    return HttpResponse('<h1> Hello SmartCity! </h1>')

def test_flutter_connection(request):
    return JsonResponse({'message': 'Hello from Django'})


