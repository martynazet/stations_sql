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


def read_file(file_path):
    with open(file_path, 'r') as f:
        next(f)
        data = []
        for line in f:
            values = line.strip().split(',')
            if 'stations' in file_path:
                record = {
                    'station': values[0],
                    'latitude': float(values[1]),
                    'longitude': float(values[2]),
                    'elevation': float(values[3]),
                    'name': values[4],
                    'country': values[5],
                    'state': values[6]
                    }
            else:
                record = {
                    'station': values[0],
                    'date': datetime.strptime(values[1], "%Y-%m-%d").date(),
                    'precip': float(values[2]),
                    'tobs': int(values[3])
                }
            data.append(record)
    return data


def read_stations():
    data_stations = read_file('clean_stations.csv')
    return data_stations


def read_measures():
    data_measures = read_file('clean_measure.csv')
    return data_measures


if __name__ == "__main__":
    conn = engine.connect()    
    conn.execute(stations.insert(), read_stations())
    conn.execute(measures.insert(), read_measures())

    result_stations = conn.execute(stations.select().limit(5)).fetchall()
    result_measures = conn.execute(measures.select().limit(5)).fetchall()
    station_update = conn.execute(stations.update().where(stations.c.station == "USC00519397").values(country="United States"))
    result_stations_update = conn.execute(stations.select().where(stations.c.station == "USC00519397")).fetchall()
    delete_measures = conn.execute(measures.delete().where(measures.c.station == "USC00513117"))
    result_delete_measures = conn.execute(measures.select().where(measures.c.station == "USC00513117")).fetchall()
    
    print(f'Stacje: {result_stations}')
    print(f'Pomiary: {result_measures}')
    print(f'Stacja: {result_stations_update}')
    print(f'Pomiar: {result_delete_measures}')