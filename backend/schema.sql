CREATE TABLE flights (
    flight_id       TEXT PRIMARY KEY,
    route           TEXT NOT NULL,
    flight_date     DATE NOT NULL,
    capacity        INT  NOT NULL CHECK (capacity > 0),
    seats_available INT  NOT NULL CHECK (seats_available >= 0 AND seats_available <= capacity),
    price_cents     BIGINT NOT NULL CHECK (price_cents >= 0)
);
CREATE INDEX idx_flights_route_date ON flights (route, flight_date);
CREATE TABLE reservations (
    reservation_id TEXT PRIMARY KEY,
    flight_id      TEXT NOT NULL REFERENCES flights(flight_id),
    passenger_name TEXT NOT NULL,
    fare_class     TEXT NOT NULL CHECK (fare_class IN ('ECONOMY','BUSINESS')),
    status         TEXT NOT NULL DEFAULT 'CONFIRMED',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_reservations_flight ON reservations (flight_id);
