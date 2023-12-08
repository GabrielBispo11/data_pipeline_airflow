-- Active: 1701980376314@@127.0.0.1@3309@cars
DROP DATABASE IF EXISTS cars;

CREATE DATABASE IF NOT EXISTS cars;

USE cars;

SELECT 'CREATING DATABASE STRUCTURE' as 'INFO';

DROP TABLE IF EXISTS owners
                     cars;

CREATE TABLE
    owners (
        id_ INT NOT NULL,
        first_name VARCHAR(35) NOT NULL,
        last_name VARCHAR(35) NOT NULL,
        country VARCHAR(35) NOT NULL,
        credit_card_type VARCHAR(35) NOT NULL,
        PRIMARY KEY (id_)
    );

CREATE TABLE
    cars (
        id_ INT NOT NULL,
        car_brand VARCHAR(20) NOT NULL,
        car_model VARCHAR(35) NOT NULL,
        car_color VARCHAR(10) NOT NULL,
        year_manufacture INT NOT NULL,
        owners_id INT,
        PRIMARY KEY (id_),
        FOREIGN KEY (owners_id) REFERENCES owners(id_)
    );