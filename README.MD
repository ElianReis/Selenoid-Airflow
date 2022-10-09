# airflow-selenoid
#### Project to integrate Airflow and Selenoid browser instances
> The idea is basically orchestrate multiple browsers for testing specific elements on
> multiple urls inputs

## How to install dependencies

To install them, run:

```
docker build . --tag airflow-selenoid
docker pull selenoid/firefox:105.0
docker-compose up
```
## How to use your own packages

Just include the packages you want on Requirements or Dockerfile


## How to see the webservers

You can see the Airflow Webserver in

```
localhost:8080
```
> user: airflow
> 
> pwd: airflow

You can see the Selenoid UI in
```
localhost:8081
```
## How it works

Using a multiple urls inputs and a specific configuration for both of them on yaml file
we will request every url and bring the element we want. 

It's a good approach for a stable multiple testing pipelines

For checking which browser we are using for testing, go to
> config/browsers.json

## Configuration of scrapers

> dags/config.yaml

## For more info:
> Check **airflow** and **selenoid** documentation:
> 
> https://airflow.apache.org/
> 
> https://aerokube.com/selenoid/latest/

## Visualization on airflow
![Alt text](airflow.png?raw=true "Airflow")

## Visualization on selenoid-grid
![Alt text](selenoid.png?raw=true "Selenoid")