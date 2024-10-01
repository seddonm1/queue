FROM postgres:17.0

RUN apt-get update &&\
    apt-get -y install postgresql-17-cron &&\
    echo "shared_preload_libraries='pg_cron'" >> /usr/share/postgresql/postgresql.conf.sample &&\
    echo "cron.database_name='postgres'" >> /usr/share/postgresql/postgresql.conf.sample