import math

from flask import Flask, request, render_template, flash, jsonify
import pyodbc
from time import time
import csv
import hashlib
import redis
import pickle as cPickle
from random import *
import random
import time
import numpy as np

app = Flask(__name__)
app.config['SESSION_TYPE'] = 'memcached'
app.config['SECRET_KEY'] = 'Secret'

connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=swethacloudserver.database.windows.net;Database=Sapp1;Uid=xxxxx;Pwd=xxxxx")

cursor = connection.cursor()
print(cursor)

r = redis.StrictRedis(host="swethacache.redis.cache.windows.net",port=6380,password='XXXX',ssl=True)


#
# @app.route('/')
# def hello_world():
#     return 'Hello !'


@app.route('/')
def ti():
    time_taken = 1553.81233311
    flash('The Avg Time taken to execute the random queries is : ' + "%.4f" % time_taken + " seconds")
    # labels = '2011','2012','2013','2014'
    # y_pos = np.arange(len(labels))
    # plt.bar(y_pos, time_taken, align='center', alpha=0.5)
    # plt.xticks(y_pos, labels)
    # plt.ylabel('time')
    # plt.show()
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


@app.route('/Limit', methods=['get', 'post'])
def limit():
    limit = request.form['limit']
    print(limit)
    print('before query')
    query1 = "Select * from dbo.all_month where locationSource='" + limit + "';"
    print('after query')
    print(query1)
    starttime = time.time()
    print(starttime)
    with connection.cursor() as cursor:
        cursor.execute(query1)
        connection.commit()
    cursor.close()
    endtime = time.time()
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
    beforeTime = time.time()
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


#normal execution time caclulation
@app.route('/update', methods=["POST"])
def update():
    mr3 = request.form.get('mrs3')
    start = time()
    cur = connection.cursor()
    cur.execute("select * from dbo.all_month where locationSource='" + mr3 + "';")
    connection.commit()
    end = time()
    ti=end - start
    return render_template("out.html", count=ti)


@app.route('/rdsquery', methods=['POST'])
def rdsquery():
    # 1000 random queries normal execution
    num = 100
    c = connection.cursor()
    sql_query_list = []
    sql1 = "SELECT latitude FROM dbo.all_month where locationSource='ak';"
    sql_query_list.append(sql1)
    sql2 = "SELECT  longitude FROM dbo.all_month where locationSource='hv';"
    sql_query_list.append(sql2)
    sql3 = "SELECT  depth FROM dbo.all_month where locationSource='ak';"
    sql_query_list.append(sql3)
    length = len(sql_query_list)
    start_time = time()
    for j in range(1, int(num)):
        randid = randint(1, int(length) - 1)
        # print randid
        sqlquery = sql_query_list[int(randid)]
        # print sql_query_list[int(randid)]
        # print sqlquery
        c.execute(sqlquery)

    end_time = time()
    time_diff = end_time - start_time
    return render_template('rdsquery.html', difference=time_diff)


@app.route('/randomgen',methods=['POST'])
def randomgen():
    ran=[]
    depth =int(request.form['depth'])
    times = int(request.form['times'])
    print(depth)
    print(times)
    c = connection.cursor()
    num1=depth-5
    num2=depth+5
    st = time()
    for i in range(1, int(times)):
        print(random, type(random))
        rand_number =random.randint(num1,num2)
        # rno = rand_number
        rno = rand_number
        print(rno)
        #sqlq = "SELECT time FROM dbo.all_month WHERE depth = %s"
        #print (sqlq)
        #c.execute(sqlq, rno)
        c.execute('SELECT time FROM dbo.all_month WHERE depth='+str(rno))
    et = time()
    time_diff = et - st
    data = c.fetchall()
    count=0
    for row in data:
        count=count+1
        #ran.append("Time: "+row[0])
    return render_template('randomgen.html',count=count,time = time_diff)


@app.route('/county', methods=['POST'])
def county():
    County = request.form['county']
    print(County)
    print('before query')
    query1= "SELECT population. *, counties.County FROM population INNER JOIN counties ON population.State = counties.State and counties.County = '" + County + "';"
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
    # return render_template('OK.html', time1=totalsqltime)

    TTL = 36
    sql = "SELECT population. *, counties.County FROM population INNER JOIN counties ON population.State = counties.State and counties.County = '" + County + "';"
    # sql = "Select * from dbo.all_month where locationSource='" + limit + "';"
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
    msg = "Sql Time: "+ str(totalsqltime) + "Redis time: " + str(Totaltime)
    return render_template('both.html', msg=msg)
    #return 'Took time : ' + str(Totaltime)



@app.route('/yearrange', methods=['GET','POST'])
def yearrange():
    res = []
    year = request.form['year']
    p1 = request.form['p1']
    p2 = request.form['p2']
    query1= "select  p.State from population p left join counties c on p.State = c.State where p.["+year+"] >=" + p1 + "  and p.["+year+"] <=" + p2 + ";"
    print(query1)
    cur = connection.cursor()
    cur.execute(query1)
     # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    count = 0
    for row in data:
        count = count + 1
        res.append("State:" + str(row[0]))
    return render_template('list2.html', res=res, count=count)

@app.route('/rms', methods=['POST'])
def rms():
    beforeTime = time()
    hits = 0;
    # LONG RANGE
    for x in range(1, 250):
        rand_number = float(random.randrange(0, 600) / 100)
        # print(rand_number)
        result = r.get("magnitude" + str(rand_number))
        if not result:
            cursor.execute('SELECT * FROM dbo.all_month where mag = ?', (rand_number))
            result = cursor.fetchall()
            # print(result)
            r.set("magnitude" + str(rand_number), cPickle.dumps(result), ex=3600)
        else:
            result = cPickle.loads(result)
            hits = hits + 1
    afterTime = time()
    timeDifference = afterTime - beforeTime
    msg = "Random Queries time=" + str(timeDifference) + " hit percentage=" + str(hits / 1000 * 100) + "\n"
    return render_template('rdsquery.html', difference=msg)

@app.route('/code', methods=['GET','POST'])
def code():
    res = []
    year = request.form['year']
    co = request.form['co']
    query1= "select p.["+year+"] from population p left join statecode s on p.State = s.State and s.code = '" + co + "';"
    print(query1)
    cur = connection.cursor()
    cur.execute(query1)
     # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    # count = 0
    for row in data:
        # count = count + 1
        res.append(str(row[0]))
    return render_template('listcode.html', res=res)

@app.route('/countydis', methods=['GET','POST'])
def countydis():
    res = []
    co = request.form['co']
    query1= "select distinct(counties.County), count(distinct(counties.County)) as count from counties left join statecode on counties.State = statecode.State where statecode.code = '" + co + "' group by (counties.County)"
    print(query1)
    cur = connection.cursor()
    cur.execute(query1)
     # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    return render_template('list_county.html', data = data)

@app.route('/showmag')
def showdate():
    cur = connection.cursor()
    rangelist = [0]
    datalist = {}
    for i in np.arange(2,6.5,.5):
        rangelist.append(i)
    for i in rangelist:
        cur.execute(" select count(*) as total from dbo.all_month where (mag between %f and %f)"% (i,(i+.5)))
        # cur.execute("select count(*) as total from dbo.all_month  where (mag between %f and %f) and (Substring(time,1,10) BETWEEN \'%s\' and \'%s\')"%(i,(i+.5),date1, date2))
        st = "Magnitude (%.2f to %.2f)" % (i, i + 0.5)
        datalist[st] = (cur.fetchone()[0])
    cur.close()
    return render_template("showres.html", datalist=datalist)

@app.route('/showyearwiseed')
def showyearwiseed():
    cur = connection.cursor()
    rangelist = [0]
    datalist = {}
    for i in np.arange(1970,2010,5):
        rangelist.append(i)
    for i in rangelist:
        cur.execute(" select count(*) as total from dbo.education where (year between %s and %s)"% (i,(i+5)))
        # cur.execute("select count(*) as total from dbo.all_month  where (mag between %f and %f) and (Substring(time,1,10) BETWEEN \'%s\' and \'%s\')"%(i,(i+.5),date1, date2))
        st = "Year (%s to %s)" % (i, i + 5)
        datalist[st] = (cur.fetchone()[0])
    cur.close()
    return render_template("showedyear.html", datalist=datalist)

@app.route('/q99', methods=['GET','POST'] )
def q99():
    cur = connection.cursor()
    code = request.form['code']
    rangelist = [0]
    datalist = {}
    for i in np.arange(1970,2010,5):
        rangelist.append(i)
    for i in rangelist:
        cur.execute(" select count(*) as total from dbo.education where education.code = '"+code+"' and  (year between %s and %s)"% (i,(i+5)))
        # cur.execute("select count(*) as total from dbo.all_month  where (mag between %f and %f) and (Substring(time,1,10) BETWEEN \'%s\' and \'%s\')"%(i,(i+.5),date1, date2))
        st = "Year (%s to %s)" % (i, i + 5)
        datalist[st] = (cur.fetchone()[0])
    cur.close()
    return render_template("showedyear.html", datalist=datalist)



@app.route('/mydat', methods=['GET','POST'])
def mydat():
    print('hi')
    res = []
    year = request.form['year']
    p1 = request.form['p1']
    p2 = request.form['p2']
    p3 = request.form['p3']
    p4 = request.form['p4']
    p5 = request.form['p5']
    p6 = request.form['p6']
    rangelist = [0]
    datalist = {}
    for i in np.arange(1970, 2010, 5):
        rangelist.append(i)
    for i in rangelist:
        cur.execute( "select  count(*) as total, p.State from population p where (p.["+year+"] >=" + p1 + "  and p.["+year+"] <=" + p2 + ") "
                                                                                                                                         "or select  count(*) as total, p.State from population p where (p.["+year+"] >=" + p3 + "  and p.["+year+"] <=" + p4 + ") "
                                                                                                                                                                                                                                                                "or select  count(*) as total, p.State from population p where (p.["+year+"] >=" + p5 + "  and p.["+year+"] <=" + p6 + " where (year between %s and %s))" % (i, (i + 5)))
        # cur.execute("select count(*) as total from dbo.all_month  where (mag between %f and %f) and (Substring(time,1,10) BETWEEN \'%s\' and \'%s\')"%(i,(i+.5),date1, date2))
        st = "Year (%s to %s)" % (i, i + 5)
        datalist[st] = (cur.fetchone()[0])
    cur.close()
    return render_template("showedyear.html", datalist=datalist)

    cur = connection.cursor()
    print(cur)
    # cur.execute("SELECT count(mag) from dbo.all_month where (mag BETWEEN 2.0 AND 2.5)")
    cur.execute("select  p.State from population p where (p.["+year+"] >=" + p1 + "  and p.["+year+"] <=" + p2 + ");")
    full1 = cur.fetchone()
    print(full1)
  # cur.execute("SELECT count(mag) from dbo.all_month where (mag BETWEEN 2.5 AND 3.0)")
    cur.execute("select  p.State from population p where (p.[" + year + "] >=" + p3 + "  and p.[" + year + "] <=" + p4 + ");")
    full2 = cur.fetchone()
    print(full2)
  # cur.execute("SELECT count(mag) from dbo.all_month where (mag BETWEEN 3.0 AND 3.5)")
    cur.execute("select  p.State from population p where (p.[" + year + "] >=" + p5 + "  and p.[" + year + "] <=" + p6 + ");")
    full3 = cur.fetchone()
    print(full3)
  # cur.execute("SELECT count(mag) from dbo.all_month where (mag BETWEEN 3.5 AND 4.0)")
  # full4 = cur.fetchone()
  # cur.execute("SELECT count(mag) from dbo.all_month where (mag BETWEEN 4.0 AND 4.5)")
  # full5 = cur.fetchone()
    rows = [full1[0], full2[0], full3[0]]
    print(rows)
    return render_template('pie.html', rows=rows)


@app.route('/y1', methods=['GET','POST'])
def y1():
    res = []
    year = request.form['year']
    p1 = request.form['p1']
    p2 = request.form['p2']

    query1= "select  p.State from population p where (p.["+year+"] >=" + p1 + "  and p.["+year+"] <=" + p2 + ");"
    cur = connection.cursor()
    cur.execute(query1)
     # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    count = 0
    for row in data:
        count = count + 1
        res.append("State:" + str(row[0]))
    return render_template('list2.html', res=res, count=count)

@app.route('/y2', methods=['GET','POST'])
def y2():
    res = []
    year = request.form['year']
    p3 = request.form['p3']
    p4 = request.form['p4']
    query2 = "select  p.State from population p where (p.["+year+"] >=" + p3 + "  and p.["+year+"] <=" + p4 + ");"
    cur = connection.cursor()
    cur.execute(query2)
     # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    count = 0
    for row in data:
        count = count + 1
        res.append("State:" + str(row[0]))
    return render_template('list2.html', res=res, count=count)

@app.route('/y3', methods=['GET','POST'])
def y3():
    res = []
    year = request.form['year']

    p5 = request.form['p5']
    p6 = request.form['p6']
    query3 = "select  p.State from population p where (p.[" + year + "] >=" + p5 + "  and p.[" + year + "] <=" + p6 + ");"
    cur = connection.cursor()
    cur.execute(query3)
     # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    count = 0
    for row in data:
        count = count + 1
        res.append("State:" + str(row[0]))
    return render_template('list2.html', res=res, count=count)

@app.route('/q9', methods=['GET','POST'])
def q9():
    res = []
    code = request.form['code']
    year1 = request.form['year1']
    year2 = request.form['year2']
    query1= "select BLPercent from education where education.code = '"+code+"' and year between "+year1+"and "+year2+""
    cur = connection.cursor()
    cur.execute(query1)
     # cur.execute('select mag,locationSource from dbo.all_month where locationSource="'+mr+'" and mag between' + mr1 + 'and' + mr2)
    data = cur.fetchall()
    print(data)
    count = 0
    for row in data:
        count = count + 1
        res.append(str(row[0]))
    return render_template('list2.html', res=res, count=count)

if __name__ == '__main__':
    app.run()
