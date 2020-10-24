from django.db import models

class Employee(models.Model):
    name = models.CharField(max_length=30)
    country = models.CharField(max_length=30)