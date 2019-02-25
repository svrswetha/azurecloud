import math

from flask import Flask, request, render_template, flash
import pyodbc
from time import time
import csv
import hashlib
import redis
import pickle as cPickle
from random import *

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'memcached'
app.config['SECRET_KEY'] = 'Secret'

connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=swethacloudserver.database.windows.net;Database=Sapp1;Uid=svrswetha@swethacloudserver;Pwd=Swetha123")

cursor = connection.cursor()
print(cursor)

r = redis.StrictRedis(host="swethacache.redis.cache.windows.net",port=6380,password='abLrdUdH00V7BjipqOZ2McUopRaOkc8hgsr8jo6PRuQ=',ssl=True)


#
# @app.route('/')
# def hello_world():
#     return 'Hello !'


@app.route('/')
def ti():
    time_taken = 1553.81233311
    flash('The Avg Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
    return render_template('index.html', t=time_taken)
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

@app.route('/Limit', methods=['get', 'post'])
def limit():
    limit = request.form['limit']
    print(limit)
    print('before query')
    query1 = "Select * from dbo.all_month where locationSource='" + limit + "';"
    print('after query')
    print(query1)
    starttime = time()
    print(starttime)
    with connection.cursor() as cursor:
        cursor.execute(query1)
        connection.commit()
    cursor.close()
    endtime = time()
    print('endtime')
    totalsqltime = endtime - starttime
    print(totalsqltime)
    return render_template('OK.html', time1=totalsqltime)

@app.route('/memexec', methods=['POST'])
def memexec():
    TTL = 36
    limit = request.form['limit']
    sql = "Select * from dbo.all_month where locationSource='" + limit + "';"
    print("I am atlast here" + sql)
    beforeTime = time()
    hash = hashlib.sha224(sql.encode('utf-8')).hexdigest()
    key = "sql_cache:" + hash
    print("Created Key\t: %s" % key)
    if(r.get(key)):
        print("it is returned from redis")
        return cPickle.loads(r.get(key))
    else:
        # Do MySQL query
        with connection.cursor() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
            connection.commit()
    # cursor.close()
    # Put data into cache for 1 hour
    r.set(key, cPickle.dumps(data))
    r.expire(key, TTL);
    print("Set data redis and return the data")
    afterTime = time()
    Totaltime = afterTime - beforeTime
    # print (str(float(Totaltime)))
    return render_template('memOK.html', time1=Totaltime)
    #return 'Took time : ' + str(Totaltime)

@app.route('/fetch', methods=['post'])
def fetch():
    #normal execution
    res = []
    mr = request.form.get('mrs')
    mr1 = request.form.get('mrs1')
    mr2 = request.form.get('mrs2')
    cur = connection.cursor()
    cur.execute("select mag, locationSource from dbo.all_month where locationSource = '"+mr+"' and mag between '"+mr1+"' and '"+mr2+"';")
    # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    count = 0
    for row in data:
        count = count + 1
        res.append("Magnitude:" + str(row[0]) + "  ; Location:" + str(row[1]))
    return render_template('list2.html', res=res, count=count)

if __name__ == '__main__':
    app.run()
