from sqlalchemy import Table, Column, Integer, Float, String, Date, MetaData, ForeignKey
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine("sqlite:///stations.db")

metadata = MetaData()

stations = Table(
    'stations', metadata,
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('name', String),
    Column('country', String),
    Column('state', String)
)

measures = Table(
    'measures', metadata,
    Column('id', Integer, primary_key=True),
    Column('station', String, ForeignKey('stations.station')),
    Column('date', Date),
    Column('precip', Float),
    Column('tobs', Integer)

)

metadata.create_all(engine)

with open('clean_stations.csv', 'r') as file:
    next(file)
    data1 = []
    for line in file:
        values = line.strip().split(',')
        station = {
            'station': values[0],
            'latitude': float(values[1]),
            'longitude': float(values[2]),
            'elevation': float(values[3]),
            'name': values[4],
            'country': values[5],
            'state': values[6]
        }
        data1.append(station)


with open('clean_measure.csv', 'r') as file:
    next(file)
    data2 = []
    for line in file:
        values = line.strip().split(',')
        measure = {
            'station': values[0],
            'date': datetime.strptime(values[1], "%Y-%m-%d").date(),
            'precip': float(values[2]),
            'tobs': int(values[3])
        }
        data2.append(measure)

conn = engine.connect()
conn.execute(stations.insert(), data1)
conn.execute(measures.insert(), data2)

result_stations = conn.execute(stations.select().limit(5)).fetchall()
result_measures = conn.execute(measures.select().limit(5)).fetchall()

print(f'Stacje: {result_stations}')
print(f'Pomiary: {result_measures}')