.PHONY : all
all:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: *.java Utils/*.java INameNode/*.java IDataNode/*.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IReducer/*.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: IMapper/*.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: IJobTracker/*.java
	protoc -I=. --java_out=./bin MapReduce.proto
	protoc -I=. --java_out=./bin hdfs.proto

Client:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: Client.java
NameNode:
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: NameNode.java
INameNode:
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: INameNode/*.java
DataNode:
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: DataNode.java
IDataNode:
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: IDataNode/*.java
JobTracker:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: JobTracker.java
IJobTracker:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IJobTracker/*.java
JobClient:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: JobClient.java
TaskTracker:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: TaskTracker.java
IMapper:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IMapper/*.java
IReducer:
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IReducer/*.java
MapProto:
	protoc -I=. --java_out=./bin MapReduce.proto
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: ./bin/MapReduceProto/*.java
HDFSProto:
	protoc -I=. --java_out=./bin hdfs.proto
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: ./bin/Protobuf/*.java
