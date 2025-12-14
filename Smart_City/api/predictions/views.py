from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import random

@csrf_exempt
def predict_traffic(request):
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=400)

    body = json.loads(request.body)
    datetime_val = body.get("datetime", None)

    if not datetime_val:
        return JsonResponse({"error": "Missing datetime"}, status=400)
    
    # Łukasz

    # tymczasowa "predykcja" – losowa liczba 1-5
    prediction = random.randint(1, 5)

    return JsonResponse({
        "datetime_received": datetime_val,
        "prediction": prediction
    })
