# Track via Streams API

This is a demo project that shows how to create a REST interface on top of MapR Streams.

To run the project do the following

1) Build and Install (create a jar file) using Maven commands

2) java -cp /tmp/uber-track-via-streams-api-0.1.jar:`mapr classpath` com.example.trackapi.Application
The above will run the application with an embedded tomcat server

3) Run the Message consumer to see the messages consumed. This is for testing
java -cp /tmp/uber-track-via-streams-api-0.1.jar:`mapr classpath` com.example.trackapi.streams.MessageConsumer /trackapp/trackstreams:eventtopic g1 c1

Param1 - Stream / Topic to consume the messages from
Param 2 - Consumer Group id
Param 3 - Consumer id

4) Example CURL command to call the REST API
curl -H "Content-Type: application/json"  -X POST -d '{"name":"value"}' http://35.163.42.148:8080/send/rest/create-sync/userid1

