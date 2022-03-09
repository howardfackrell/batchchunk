# batchchunk

___
Example application to experiment with Spring Batch's chunking and fault tolerance settings

## Prereqs

We'll assume you are familiar with and have several things installed

- [Java 17](https://sdkman.io)
- [Docker](https://hub.docker.com)
- [Spring boot](https://spring.io/projects/spring-boot)
- [Spring batch](https://spring.io/projects/spring-batch)
- [Postgres](https://www.postgresql.org/docs/) and [SQL](https://zaiste.net/posts/postgresql-primer-for-busy-people/)

## Setup

Compile the code

```shell
./mvnw clean compile
```

Run postgres in a docker container (see docker-compose.yml)

```shell
docker-compose up
```

## Running the app

The app will setup, start & run a single Spring Batch job. There are several ways you could run the app, choose whatever
suits your needs.

To run the app using maven:

```shell
./mvnw spring-boot:run
```

## Experiment

Now, open the JobRunner class. This is intended to be one-stop-shopping for all the configuration you might want to
adjust.

There are some simple Spring Batch ItemReaders, ItemProcessors, and ItemWriters defined in the Readers, Processors and
Writers classes. Some of them are defined as static factory methods, some of them require spring wiring so they are
defined as spring beans.

1. Change the Job configuration in JobRunner
2. Run the Job
3. Examine the results. Depending on your configuration that might be console logs or the work_log table in the database
4. Repeat steps 1-3