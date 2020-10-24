from django.shortcuts import render
from rest_framework import viewsets
from .models import Employee
from .serializers import EmployeeSerializer
from django.http import JsonResponse
from rest_framework.decorators import api_view
from rest_framework.response import Response
from utilities.table_analyzer import Db2Analyzer


@api_view(['GET'])
def table_analyzer(request):
    db2_obj = Db2Analyzer()
    db2_obj.download_files()
    db2_obj.db2_analyzer()
    return JsonResponse("Samskriti Sharma", safe=False)

    # employees = Employee.objects.all()
    # serializer = EmployeeSerializer(employees, many=True)
    # return Response(serializer.data)
