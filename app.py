from flask import Flask, request, render_template, flash
import pyodbc
from time import time
import csv


app = Flask(__name__)
app.config['SESSION_TYPE'] = 'memcached'
app.config['SECRET_KEY'] = 'Secret'

connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=swethacloudserver.database.windows.net;Database=Sapp1;Uid=svrswetha@swethacloudserver;Pwd=Swetha123")

cursor = connection.cursor()
print(cursor)


@app.route('/')
def hello_world():
    return 'Hello !'


# @app.route('/')
# def index():
#     start_time = time()
#     cursor.execute("CREATE TABLE [dbo].[all_month](\
#     	[time] [datetime2](7) NULL,\
#     	[latitude] [float] NULL,\
#     	[longitude] [float] NULL,\
#     	[depth] [float] NULL,\
#     	[mag] [float] NULL,\
#     	[magType] [nvarchar](50) NULL,\
#     	[nst] [int] NULL,\
#     	[gap] [float] NULL,\
#     	[dmin] [float] NULL,\
#     	[rms] [float] NULL,\
#         [net][nvarchar](50) NULL,\
#         [id][nvarchar](50) NULL,\
#         [updated] [datetime2](7) NULL,\
#         [place][nvarchar](100) NULL,\
#         [type][nvarchar](50) NULL,\
#         [horizontalError][float] NULL,\
#         [depthError][float] NULL,\
#         [magError][float] NULL,\
#         [magNst][int] NULL,\
#         [status][nvarchar](50) NULL,\
#         [locationSource][nvarchar](50) NULL,\
#         [magSource][nvarchar](50) NULL)")
#     connection.commit()
#
#     query = "INSERT INTO dbo.all_month (time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontalError,depthError,magError,magNst,status,locationSource,magSource) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
#
#     with open('all_month.csv') as csvfile:
#           next(csvfile)
#           reader = csv.reader(csvfile, delimiter=',')
#           for row in reader:
#               print(row)
#               cursor.execute(query,row)
#
#           connection.commit()
#     end_time = time()
#     time_taken = (end_time - start_time)
#     flash('The Avg Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
#     return render_template('index.html', t=time_taken)
#

if __name__ == '__main__':
    app.run()
