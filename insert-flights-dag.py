from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import psycopg2
import os

aviationstack_api_key = os.environ['AVIATIONSTACK_API_KEY']
postgres_pass = os.environ['POSTGRES_PASSWORD']

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
    """Converts raw json flight data to an object of the class Flight

    Parameters
        ----------
        data : dict
        Raw json flight data

    Raises
    ------
        Exception
        If flight number is None        
    """
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

def transform_flight(flight):
    """Transforms flight object properties arrival terminal and arrival timezone formats from / to -

    Parameters
        ----------
        flight : Flight
        Flight object

    Raises
    ------
        Exception
        If any property is None
    """
    try:
        flight.arrival.terminal = flight.arrival.terminal.replace("/", " - ")
        flight.departure.timezone = flight.departure.timezone.replace("/", " - ")
        return flight
    except Exception as e:
        raise(e)

def insert_flights():
    try:
        print("Opening connection")
        conn = psycopg2.connect(dbname='testfligoo', user='fligoo', password=postgres_pass, host='fligoo-db', port='5432')
        cursor = conn.cursor()

        print("Sending aviation stack request")
        response = requests.get(f'https://api.aviationstack.com/v1/flights?access_key={aviationstack_api_key}&flight_status=active&limit=100')
        data = response.json()["data"]
        flights = []
        print("Parsing flights values")
        for d in data:
            try:
                flight = parse_flight(d)
                transformed_flight = transform_flight(flight)
                flights.append(transformed_flight)
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

        print("Preparing values to insert")
        args = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s)", value).decode('utf-8')
                for value in insert_values)
        print("Executing insert")
        cursor.execute("INSERT INTO testdata (flight_number, flight_status, flight_date, departure_airport, departure_timezone, arrival_airport, arrival_timezone, arrival_terminal, airline_name) VALUES " + (args) + " ON CONFLICT DO NOTHING")
        print("Insert executed")

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