.PHONY : all
all:
	mkdir bin
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: *.java Utils/*.java INameNode/*.java IDataNode/*.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IReducer/*.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: IMapper/*.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: IJobTracker/*.java
	protoc -I=. --java_out=./bin MapReduce.proto
	protoc -I=. --java_out=./bin hdfs.proto
	cp protobuf-java-3.0.2.jar ./bin/
Client: Client.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: Client.java
NameNode: NameNode.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: NameNode.java
INameNode: INameNode/*.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: INameNode/*.java
DataNode: DataNode.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: DataNode.java
IDataNode: IDataNode/*.java
	javac -d ./bin/ -cp ./protobuf-java-3.0.2.jar: IDataNode/*.java
JobTracker: JobTracker.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: JobTracker.java
IJobTracker: IJobTracker/*.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IJobTracker/*.java
JobClient: JobClient.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: JobClient.java
TaskTracker: TaskTracker.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: TaskTracker.java
IMapper: IMapper/*.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IMapper/*.java
IReducer: IReducer/*.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: IReducer/*.java
MapProto: MapReduce.proto
	protoc -I=. --java_out=./bin MapReduce.proto
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: ./bin/MapReduceProto/*.java
HDFSProto:  hdfs.proto
	protoc -I=. --java_out=./bin hdfs.proto
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: ./bin/Protobuf/*.java
Utils: Utils/*.java
	javac -d ./bin/  -cp ./protobuf-java-3.0.2.jar: Utils/*.java
CopyFiles:
	cp README bin/
clean:
	rm -r ./bin/
