from django.db import models

class RZTraffic(models.Model):
    id = models.AutoField(primary_key=True)
    measuring_point = models.CharField(max_length=50)
    location = models.CharField(max_length=100)
    approach = models.CharField(max_length=50)
    direction = models.CharField(max_length=50)
    date_time = models.DateTimeField()
    traffic_count = models.IntegerField()

    class Meta:
        db_table = 'rz_traffic'     
        managed = False

class alerts_and_news(models.Model):
    id = models.AutoField(primary_key=True)
    date_time = models.DateTimeField()
    street_name = models.CharField(max_length=50)
    alert_type = models.CharField(max_length=50)
    alert_description = models.CharField(max_length=100)

    class Meta:
        db_table = 'rz_alerts'
        managed = True

class RZWeather(models.Model):
    timestamp = models.DateTimeField(primary_key=True)
    
    temperature_2m = models.FloatField(null=True, blank=True)
    dew_point_2m = models.FloatField(null=True, blank=True)
    precipitation = models.FloatField(null=True, blank=True)
    rain = models.FloatField(null=True, blank=True)
    snowfall = models.FloatField(null=True, blank=True)
    wind_speed_10m = models.FloatField(null=True, blank=True)
    wind_direction_10m = models.FloatField(null=True, blank=True)
    wind_gusts_10m = models.FloatField(null=True, blank=True)
    cloud_cover = models.SmallIntegerField(null=True, blank=True)
    shortwave_radiation = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = "rz_weather"

    def __str__(self):
        return f"{self.timestamp}"