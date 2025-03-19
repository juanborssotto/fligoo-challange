create type flight_status as enum('scheduled', 'active', 'landed', 'cancelled', 'incident', 'diverted');

create table testdata(
    flight_number int primary key,
    flight_status flight_status,
    flight_date date,
    departure_airport varchar(100),
    departure_timezone varchar(60),
    arrival_airport varchar(100),
    arrival_timezone varchar(60),
    arrival_terminal varchar(100),
    airline_name varchar(100)
);
