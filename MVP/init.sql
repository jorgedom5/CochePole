DROP TABLE IF EXISTS clientes, coordenadas, viajes, vehiculos, conductores;

CREATE TABLE vehiculos (
    vehiculo_id SERIAL PRIMARY KEY,
    coche VARCHAR(50)
);

CREATE TABLE viajes (
    viaje_id SERIAL PRIMARY KEY,
    ruta VARCHAR(50)
);

CREATE TABLE conductores (
    conductor_id SERIAL PRIMARY KEY,
    nickname VARCHAR(50),
    viaje_id INT,
    vehiculo_id INT,
    FOREIGN KEY (viaje_id) REFERENCES viajes(viaje_id),
    FOREIGN KEY (vehiculo_id) REFERENCES vehiculos(vehiculo_id)
);

CREATE TABLE coordenadas (
    coordenada_id SERIAL PRIMARY KEY,
    viaje_id INT,
    punto_ruta INT,
    latitud FLOAT,
    longitud FLOAT,
    altura FLOAT,
    distancia FLOAT,
    FOREIGN KEY (viaje_id) REFERENCES viajes(viaje_id)
);

CREATE TABLE clientes (
    cliente_id SERIAL PRIMARY KEY,
    nickname VARCHAR(50),
    coordenada_id INT,
    FOREIGN KEY (coordenada_id) REFERENCES coordenadas(coordenada_id)
);
