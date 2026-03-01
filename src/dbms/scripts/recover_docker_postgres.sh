echo 251314 | sudo -S cp docker/default_postgresql_conf/postgresql.conf docker/postgres-13-data/postgresql.conf
echo 251314 | sudo -S cp docker/default_postgresql_conf/postgresql.auto.conf docker/postgres-13-data/postgresql.auto.conf
echo 251314 | sudo -S cp docker/default_postgresql_conf/pg_hba.conf docker/postgres-13-data/pg_hba.conf
echo 251314 | sudo -S docker restart postgres-13
sleep 2

sudo docker exec -it postgres-13 psql -U postgres -c "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';"
echo 251314 | sudo -S docker restart postgres-13
sleep 2