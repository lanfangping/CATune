echo "Drop database volumes"
sudo rm -rf docker/postgres-13-data
sleep 1
echo "Stop DB container"
bash docker/stop.sh
sleep 2
echo "Start new DB container"
bash docker/start.sh
sleep 2