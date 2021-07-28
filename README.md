<h1 align="center"> Spark Test </h1> <br>

<p align="center">
TODO: Replace with Description
</p>


## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Requirements](#requirements)
- [Quick Start](#quick-start)
- [Testing](#testing)
- [API](#requirements)
- [Acknowledgements](#acknowledgements)

## Features
TODO: Description of features

* Include a list of
* all the many beautiful
* web server features

## Requirements
The application can be run locally or in a docker container, the requirements for each setup are listed below.

### Local
* [Java 8 SDK](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Maven](https://maven.apache.org/download.cgi)

### Docker
* [Docker](https://www.docker.com/get-docker)


### Run Local
First compile the JAR file
```bash
mvn clean install
```
```bash
java -jar target\sparktest-jar-with-dependencies.jar
```

### Run Docker

First build the image:
```bash
docker build --tag sparktest:latest .
```

When ready, run it:
```bash
docker run -d --memory=6g --name sparktesting -p 9080:9080 sparktest:latest
```

## Testing
TODO: Additional instructions for testing the application.

## Acknowledgements
Isaac Sanabria García