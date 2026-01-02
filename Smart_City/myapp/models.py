from django.db import models

# Create your models here.

class SensorsStates(models.Model):
    
    measuring_point = models.TextField(primary_key=True)

    sensor_state = models.TextField(null=True, blank=True)
    last_registered_malfunction = models.TextField(null=True, blank=True)
    last_update = models.DateTimeField(null=False)

    class Meta:
        db_table = "sensor_states"