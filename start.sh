if [ $SERVER_PORT ];
THEN
  /usr/local/src/*/bin/java -Dserver.port=$SERVER_PORT -jar /usr/local/src/tailbaseSampling-1.0-SNAPSHOT.jar &
ELSE
  /usr/local/src/*/bin/java -Dserver.port=8000 -jar /usr/local/src/tailbaseSampling-1.0-SNAPSHOT.jar &
iF
tail -f /usr/local/src/start.sh