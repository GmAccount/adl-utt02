# adl-utt Fitrage des clients sur un cluster on-premise

### start celery worker
cd /home/hadoop/userapi;/usr/local/bin/celery  -A tasks worker --loglevel=INFO 


#### start API

cd /home/hadoop/userapi/;uvicorn main2:app --host 0.0.0.0 --port 8001 --reload


#### making request

curl -X POST \
  http://host03.adl-utt.net:8001/items/ \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: bb881c05-2e76-41cf-9383-7aaff1fe55fa' \
  -d '{
"id_grp":7,
 "year":2021,
 "cc":"US"
}'


####  request status

curl -X GET \
  http://host03.adl-utt.net:8001/tasks/c93c60e4-87ef-4c40-a60e-5cf528d35ee8 \
  -H 'Cache-Control: no-cache' \
  -H 'Postman-Token: b2e7a911-97e9-42a7-9ec5-c6362d22a760'