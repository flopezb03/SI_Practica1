import mysql.connector
import pandas as pd
import numpy as np
import random
import matplotlib.pyplot as plt

def format_date(csv_date):
    if csv_date is None:
        return None
    
    csv_date = csv_date.strip()
    arr = csv_date.split(" ")

    year = arr[2]
    day = arr[1][:-1]
    if len(day)==1:
        day = "0"+day
    month = arr[0]
    match month:
        case "January":
            month = "01"
        case "February":
            month = "02"
        case "March":
            month = "03"
        case "April":
            month = "04"
        case "May":
            month = "05"
        case "June":
            month = "06"
        case "July":
            month = "07"
        case "August":
            month = "08"
        case "September":
            month = "09"
        case "October":
            month = "10"
        case "November":
            month = "11"
        case "December":
            month = "12"

    return year+"-"+month+"-"+day
def format_duration(csv_duration):
    if csv_duration is None:# or csv_duration is pd._libs.missing.NAType:
        return None
    
    csv_duration = csv_duration.strip()
    arr = csv_duration.split(" ")
    return np.int16(arr[0])

# Conectar con mysql
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="si_prac1_db"
)
cursor = conn.cursor()


# Leer CSV
csv = pd.read_csv("netflix_titles.csv")

# Crear tabla en mysql
sql_query = """CREATE TABLE IF NOT EXISTS peliculas_series(
show_id VARCHAR(5) PRIMARY KEY,
type VARCHAR(7),
title VARCHAR(1024),
director VARCHAR(1024),
cast VARCHAR(1024),
country VARCHAR(128),
date_added DATE,
release_year SMALLINT,
rating VARCHAR(8),
duration SMALLINT,
listed_in VARCHAR(1024),
description VARCHAR(1024)
);
"""
cursor.execute(sql_query)
conn.commit()

#   Limpiar CSV
csv = csv.where(pd.notnull(csv), None)  # Pasar valores np.nan a NoneType para que sql los pase a NULL
for i in range(len(csv)):
    csv.loc[i,"date_added"] = format_date(csv.loc[i,"date_added"])
    csv.loc[i,"duration"] = format_duration(csv.loc[i,"duration"])

sql_query = "SELECT COUNT(*) FROM peliculas_series;"
cursor.execute(sql_query)
rows_peliculas_series = cursor.fetchall()[0][0]

if rows_peliculas_series == 0:
    print("La tabla 'peliculas_series' esta vacia, se esta rellenando con 'netflix_titles.csv', espere unos segundos...")
    # Pasar datos del csv a mysql
    for i in range(len(csv)):
        row = list(csv.iloc[i])
        row[7] = int(row[7]) if row[7] is not None else None
        row[9] = int(row[9]) if row[9] is not None else None

        sql_query = "INSERT INTO 'peliculas_series' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        cursor.execute(sql_query, row)
        conn.commit()
    
    sql_query = "SELECT COUNT(*) FROM peliculas_series;"
    cursor.execute(sql_query)
    rows_peliculas_series = cursor.fetchall()[0][0]
else: 
    print("Se han detectado entradas en la tabla 'peliculas_series'")

csv["release_year"] = csv["release_year"].astype("Int16")
csv["duration"] = csv["duration"].astype("Int16")



sql_query = "SELECT COUNT(*) FROM usuarios;"
cursor.execute(sql_query)
rows_usuarios = cursor.fetchall()[0][0]

if rows_usuarios==0:
    print("La tabla 'usuarios' esta vacia, se esta rellenando con valores aleatorios, espere...")
    n_users = random.randint(20,30)
    name_list = ["Jorge","Gerardo","Daniel","Lidia","Julia","Sergio","Victor","Enrique",
                 "Alvaro","Alonso","Ivan","Paula","Natalia","Marcos","Alejandro","Fernando",
                 "Jose"]
    max_year = 2025
    min_year = 2000
    for i in range(n_users):
        uid = i
        name = name_list[random.randint(0,len(name_list)-1)]
        year = str(random.randint(min_year,max_year))
        month = str(random.randint(1,12))
        match month:
            case "1" | "3" | "5" | "7" | "8" | "10" | "12":
                max_day = 31
            case "4" | "6" | "9" | "11":
                max_day = 30
            case "2":
                max_day = 28
        day = str(random.randint(1,max_day))
        hour = str(random.randint(0,23))
        minutes = str(random.randint(0,59))
        seconds = str(random.randint(0,59))

        month = '0'+month if len(month) == 1 else month
        day = '0'+day if len(day) == 1 else day
        hour = '0'+hour if len(hour) == 1 else hour
        minutes = '0'+minutes if len(minutes) == 1 else minutes
        seconds = '0'+seconds if len(seconds) == 1 else seconds

        datetime = year+'-'+month+'-'+day+' '+hour+'-'+minutes+'-'+seconds

        row = [uid,name,datetime]

        sql_query = "INSERT INTO usuarios VALUES (%s, %s, %s);"
        cursor.execute(sql_query, row)
        conn.commit()
    
    sql_query = "SELECT COUNT(*) FROM usuarios;"
    cursor.execute(sql_query)
    rows_usuarios = cursor.fetchall()[0][0]
else:
    print("Se han detectado entradas en la tabla 'usuarios'")



sql_query = "SELECT COUNT(*) FROM visionados;"
cursor.execute(sql_query)
rows_visionados = cursor.fetchall()[0][0]

if rows_visionados == 0:
    print("La tabla 'visionados' esta vacia, se esta rellenando con valores aleatorios, espere...")

    vid = 0
    for user in range(rows_usuarios):
        n_vis = random.randint(1,200)
        psids = []
        for i in range(n_vis):
            psid = random.randint(0,199)
            if not(psid in psids):
                psids.append(psid)

                row = [vid,csv.loc[psid,"show_id"],user,random.randint(1,10)]

                sql_query = "INSERT INTO visionados VALUES (%s, %s, %s, %s);"
                cursor.execute(sql_query, row)
                conn.commit()
                vid += 1
else:
    print("Se han detectado entradas en la tabla 'visionados'")



# Ej2.a
print("Ejercicio 2.a")
print("Numero de valores no missing: "+str(csv.count().sum()))
print()


# Ej2.b
print("Ejercicio 2.b")
sql_query = "SELECT AVG(duration),STDDEV(duration) FROM peliculas_series;"
cursor.execute(sql_query)
res = cursor.fetchall()
df = pd.DataFrame(res, columns=["Media", "Desviacion Estandar"])
print(df)
print("No tienen sentido los calculos porque algunos datos hacen referencia a minutos de duracion en las peliculas y otros al numero de temporadas en las series")
print()


#Ej2.c
print("Ejercicio 2.c")
sql_query = """SELECT 
    MIN(duration), 
    MAX(duration),
    MIN(release_year), 
    MAX(release_year)
FROM peliculas_series;
"""
cursor.execute(sql_query)
res = cursor.fetchall()
df = pd.DataFrame(res, columns=["Valor minimo duracion", "Valor maximo duracion", "Valor minimo anno", "Valor maximo anno"])
print(df)
print("Valor minimo y maximo de duracion no tiene sentido por el motivo anterior")
print()

# Ej3.a
print("Ejercicio 3.a")
sql_query = """
SELECT 
    type,
    COUNT(duration),
    SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END),
    AVG(duration),
    VARIANCE(duration),
    MIN(duration),
    MAX(duration)
FROM peliculas_series
GROUP BY type;
"""
cursor.execute(sql_query)
res = cursor.fetchall()
# Calculo por separado de la mediana usando pandas porque sql no lo permite
df_median = csv.groupby("type")["duration"].median().reset_index()
df_median.columns = ["type", "Mediana"]
# Union de salidas a un unico Dataframe
cols = ["type", "Numero de Observaciones", "Numero de Valores Missing", "Media", "Varianza", "Minimo", "Maximo"]
df_sql = pd.DataFrame(res, columns=cols)
df = df_sql.merge(df_median, on="type", how="left")

print("Agrupacion por contenido")
print(df)
print()


# Ej3.b
print("Ejercicio 3.b")
sql_query = """
SELECT 
    CASE 
        WHEN duration <= 2 THEN '<=2 Temporadas'
        ELSE '>2 Temporadas'
    END AS type_range,
    COUNT(duration),
    SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END),
    AVG(duration),
    VARIANCE(duration),
    MIN(duration),
    MAX(duration)
FROM peliculas_series
WHERE type = 'TV Show'
GROUP BY type_range;
"""
cursor.execute(sql_query)
res = cursor.fetchall()
# Calculo por separado de la mediana usando pandas porque sql no lo permite
csv_tv_shows = csv[csv["type"] == "TV Show"].copy().reset_index(drop=True)
for i in range(len(csv_tv_shows)):
    if csv_tv_shows.loc[i,"duration"] <= 2:
        csv_tv_shows.loc[i,"type"] = "<=2 Temporadas"
    else:
        csv_tv_shows.loc[i,"type"] = ">2 Temporadas"
df_median = csv_tv_shows.groupby("type")["duration"].median().reset_index()
df_median.columns = ["type", "Mediana"]
# Union de salidas a un unico Dataframe
cols = ["type", "Numero de Observaciones", "Numero de Valores Missing", "Media", "Varianza", "Minimo", "Maximo"]
df_sql = pd.DataFrame(res, columns=cols)
df = df_sql.merge(df_median, on="type", how="left")

print("Agrupacion por temporadas (Series)")
print(df)
print()

# Ej3.c
print("Ejercicio 3.c")
sql_query = """
SELECT 
    CASE 
        WHEN duration > 90 THEN '>90 min'
        ELSE '<=90 min'
    END AS type_range,
    COUNT(duration),
    SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END),
    AVG(duration),
    VARIANCE(duration),
    MIN(duration),
    MAX(duration)
FROM peliculas_series
WHERE type = 'Movie'
GROUP BY type_range;
"""
cursor.execute(sql_query)
res = cursor.fetchall()

# Calculo por separado de la mediana usando pandas porque sql no lo permite
csv_movies = csv[csv["type"] == "Movie"].copy().reset_index(drop=True)
for i in range(len(csv_tv_shows)):
    if csv_movies.loc[i,"duration"] <= 90:
        csv_movies.loc[i,"type"] = "<=90 min"
    else:
        csv_movies.loc[i,"type"] = ">90 min"
df_median = csv_movies.groupby("type")["duration"].median().reset_index()
df_median.columns = ["type", "Mediana"]
# Union de salidas a un unico Dataframe
cols = ["type", "Numero de Observaciones", "Numero de Valores Missing", "Media", "Varianza", "Minimo", "Maximo"]
df_sql = pd.DataFrame(res, columns=cols)
df = df_sql.merge(df_median, on="type", how="left")

print("Agrupacion por duracion (Peliculas)")
print(df)
print()


# Ej5.a
print("Ejercicio 5.a")
sql_query = """
SELECT 
    peliculas_series.show_id,
    count(visionados.vid) AS visionados,
    peliculas_series.title
FROM peliculas_series JOIN visionados ON peliculas_series.show_id = visionados.show_id
WHERE peliculas_series.type = 'Movie'
GROUP BY peliculas_series.show_id
ORDER BY visionados DESC
LIMIT 10;
"""
cursor.execute(sql_query)
res = cursor.fetchall()

cols = ["show_id","visionados","title"]
df_sql = pd.DataFrame(res, columns=cols)

print(df_sql[["show_id","title"]])

plt.bar(df_sql.transpose().iloc[0],df_sql.transpose().iloc[1])
plt.title('Top 10 peliculas')
plt.xlabel('Peliculas')
plt.ylabel('Visionados')
plt.show()
print()


# Ej5.b
print("Ejercicio 5.b")
sql_query = """
SELECT 
    peliculas_series.show_id,
    count(visionados.vid) AS visionados,
    peliculas_series.title
FROM peliculas_series JOIN visionados ON peliculas_series.show_id = visionados.show_id
WHERE peliculas_series.type = 'TV Show'
GROUP BY peliculas_series.show_id
ORDER BY visionados DESC
LIMIT 10;
"""
cursor.execute(sql_query)
res = cursor.fetchall()

cols = ["show_id","visionados","title"]
df_sql = pd.DataFrame(res, columns=cols)

print(df_sql[["show_id","title"]])

plt.bar(df_sql.transpose().iloc[0],df_sql.transpose().iloc[1])
plt.title('Top 10 series')
plt.xlabel('Series')
plt.ylabel('Visionados')
plt.show()
print()

# Ej5.c
print("Ejercicio 5.c")
sql_query = """
SELECT 
    CASE
        WHEN subquery.duration > 90 THEN '>90 min'
        ELSE '<=90 min'
    END AS type_range,
    AVG(subquery.v_count)
FROM (
    SELECT peliculas_series.show_id, peliculas_series.duration, COUNT(visionados.vid) AS v_count
    FROM peliculas_series 
    JOIN visionados ON peliculas_series.show_id = visionados.show_id
    WHERE peliculas_series.type = 'Movie'
    GROUP BY peliculas_series.show_id
) AS subquery
GROUP BY type_range;
"""
cursor.execute(sql_query)
res = cursor.fetchall()

cols = ["Agrupacion","Media visionados"]
df_sql = pd.DataFrame(res, columns=cols)
print(df_sql)
print()


# Ej5.d
print("Ejercicio 5.d")
sql_query = """
SELECT 
    CASE 
        WHEN duration <= 2 THEN '<=2 Temporadas'
        ELSE '>2 Temporadas'
    END AS type_range,
    AVG(subquery.v_count)
FROM (
    SELECT peliculas_series.show_id, peliculas_series.duration, COUNT(visionados.vid) AS v_count
    FROM peliculas_series 
    JOIN visionados ON peliculas_series.show_id = visionados.show_id
    WHERE peliculas_series.type = 'TV Show'
    GROUP BY peliculas_series.show_id
) AS subquery
GROUP BY type_range;
"""
cursor.execute(sql_query)
res = cursor.fetchall()

cols = ["Agrupacion","Media visionados"]
df_sql = pd.DataFrame(res, columns=cols)
print(df_sql)
print()