from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import psycopg2

args = {
    'owner': 'Juan Cruz Borssotto',
    'start_date': days_ago(1)
}

dag = DAG(
    dag_id='insert-flights-dag',
    default_args=args,
    schedule_interval='@daily'
)

class Departure:
    def __init__(self, airport, timezone):
        self.airport = airport
        self.timezone = timezone

class Arrival:
    def __init__(self, airport, timezone, terminal):
        self.airport = airport
        self.timezone = timezone
        self.terminal = terminal

class Airline:
    def __init__(self, name):
        self.name = name

class Flight:
    def __init__(self, number, date, status, departure, arrival, airline):
        self.number = number
        self.date = date
        self.status = status
        self.departure = departure
        self.arrival = arrival
        self.airline = airline

def parse_flight(data):
    try:
        if data["flight"]["number"] is None:
            raise Exception("Null flight number") 

        return Flight(
            number=data["flight"]["number"],
            date=data["flight_date"],
            status=data["flight_status"],
            departure=Departure(airport=data["departure"]["airport"], timezone=data["departure"]["timezone"]),
            arrival=Arrival(airport=data["arrival"]["airport"], timezone=data["arrival"]["timezone"], terminal=data["arrival"]["terminal"]),
            airline=Airline(name=data["airline"]["name"])
        )
    except Exception as e:
        raise Exception(e)

def insert_flights():
    try:
        conn = psycopg2.connect(dbname='testfligoo', user='fligoo', password='mypass', host='fligoo-db', port='5432')
        cursor = conn.cursor()

        api_key = "da511f009f76904cf15aea2ac1d02e60"
        response = requests.get(f'https://api.aviationstack.com/v1/flights?access_key={api_key}&flight_status=active&limit=100')
        data = response.json()["data"]
        flights = []
        for d in data:
            try:
                flight = parse_flight(d)
                flights.append(flight)
            except Exception as e:
                print("error parsing flight: ", e, "data: ", d)
        
        insert_values = []
        for flight in flights:
            insert_values.append((
                flight.number,
                flight.status,
                flight.date,
                flight.departure.airport,
                flight.departure.timezone,
                flight.arrival.airport,
                flight.arrival.timezone,
                flight.arrival.terminal,
                flight.airline.name))

        args = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", value).decode('utf-8')
                for value in insert_values)
        print("number", flights[0].number)
        print(args)
        cursor.execute("INSERT INTO testdata (flight_number, flight_status, flight_date, departure_airport, departure_timezone, arrival_airport, arrival_timezone, arrival_terminal, airline_name) VALUES " + (args) + " ON CONFLICT DO NOTHING")

        cursor.execute("select * from testdata")

        print(cursor.fetchall())

    except Exception as e:
        print(e)
    finally:
        conn.commit()
        conn.close()

with dag:
    insert_flights_dag = PythonOperator(
        task_id='insert_flights',
        python_callable=insert_flights,
    )