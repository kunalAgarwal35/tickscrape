import pandas as pd
import numpy as np
import xlwings as xw
import os


employee = pd.DataFrame({'name': ['John Smith', 'Jane Doe', 'Mary Johnson'],
                            'department': ['Accounting', 'IT', 'IT'],
                            'hire_date': [pd.Timestamp('2008-01-01'),
                                            pd.Timestamp('2012-05-01'),
                                            pd.Timestamp('2014-02-01')],
                            'salary': [50000, 60000, 65000]})

def remove_employees_before(date, employee):
    employee = employee[employee['hire_date'] >= date]
    return employee

def add_employee(name, department, hire_date, salary, employee):
    employee = employee.append({'name': name, 'department': department, 'hire_date': hire_date, 'salary': salary}, ignore_index=True)
    return employee

# figure out increments
def get_increment(employee):
    # get the max salary
    max_salary = employee['salary'].max()
    # get the min salary
    min_salary = employee['salary'].min()
    # get the difference
    diff = max_salary - min_salary
    # divide by 5
    increment = diff / 5
    return increment

def get_salary_range(employee):
    # get the max salary
    max_salary = employee['salary'].max()
    # get the min salary
    min_salary = employee['salary'].min()
    # get the difference
    diff = max_salary - min_salary
    # divide by 5
    increment = diff / 5
    # get the salary ranges
    salary_range = [min_salary, min_salary+increment, min_salary+increment*2, min_salary+increment*3, min_salary+increment*4, max_salary]
    return salary_range


