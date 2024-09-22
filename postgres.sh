docker run \
--detach \
-it \
--rm \
--name postgres \
--tmpfs /var/lib/postgresql/data:rw \
--env POSTGRES_USER=postgres \
--env POSTGRES_PASSWORD=postgres \
--env POSTGRES_DB=postgres \
--publish 5432:5432 \
postgres:16.4 \
-c fsync=off -c full_page_writes=off -c max_connections=500