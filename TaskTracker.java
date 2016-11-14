import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;
import java.io.*;

import IJobTracker.*;
import INameNode.*;
import IDataNode.*;
import Utils.*;
import com.google.protobuf.ByteString;

import Protobuf.HDFS.ReadBlockRequest;
import Protobuf.HDFS.ReadBlockResponse;

import MapReduceProto.MapReduce.*;

public class TaskTracker {
	private static IJobTracker jobTracker;
	private static INameNode nameNode;
	private static Client client;

	private static int taskTrackerId;
	private static int numMapSlotsFree = 1;
	private static int numReduceSlotsFree = 1;

	private static final int STATUS_OK = 1;
	private static final int STATUS_NOT_OK = 0;

	private static final int JOB_FINISH = 2;
	private static final int JOB_STARTED = 1;
	private static final int JOB_WAIT = 0;
	
	public static void main(String args[]) {
		taskTrackerId = 1;
		try {
			Registry registry = LocateRegistry.getRegistry();
			jobTracker = (IJobTracker) registry.lookup("jobtracker");
			nameNode = (INameNode) registry.lookup("namenode");
			client = new Client();
		} catch (Exception e) {
			e.printStackTrace();
		}
		while (true) {
			Scanner in = new Scanner(System.in);
			String line = in.nextLine();
			if (line.equals("beat"))
				heartBeat();
		}
	}

	private static void heartBeat() {
		HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest.newBuilder();
		heartBeatRequest.setTaskTrackerId(taskTrackerId);
		heartBeatRequest.setNumMapSlotsFree(numMapSlotsFree);
		heartBeatRequest.setNumReduceSlotsFree(numReduceSlotsFree);

		try {
			byte[] hBeatResponse = jobTracker.heartBeat(Utils.serialize(heartBeatRequest.build()));
			HeartBeatResponse heartBeatResponse = (HeartBeatResponse) Utils.deserialize(hBeatResponse);
			if (heartBeatResponse.getStatus() == STATUS_OK) {
				List<MapTaskInfo> mapTaskInfos = heartBeatResponse.getMapTasksList();
				List<ReducerTaskInfo> reducerTaskInfos = heartBeatResponse.getReduceTasksList();

				for (MapTaskInfo mapTaskInfo : mapTaskInfos) {
					doMapTask(mapTaskInfo);
				}

				for (ReducerTaskInfo reducerTaskInfo : reducerTaskInfos) {
					doReduceTask(reducerTaskInfo);
				}
			} else {
				// error
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void doMapTask(MapTaskInfo mapTaskInfo) {
		int jobId = mapTaskInfo.getJobId();
		int taskId = mapTaskInfo.getTaskId();
		String mapName = mapTaskInfo.getMapName();
		List<BlockLocations> locations = mapTaskInfo.getInputBlocksList();
		String fileContent = "";
		try {
			for (BlockLocations location : locations) {
				DataNodeLocation dataNodeLocation = location.getLocations(0);
				String dnIP = dataNodeLocation.getIp();
				int dnPort = dataNodeLocation.getPort();
				int blockNumber = location.getBlockNumber();

				Registry registry = LocateRegistry.getRegistry();
	        	IDataNode dataNode = (IDataNode) registry.lookup("datanode");

	        	ReadBlockRequest.Builder readBlockRequest = ReadBlockRequest.newBuilder();
	        	readBlockRequest.setBlockNumber(blockNumber);

	        	byte[] rBlockResponse = dataNode.readBlock(Utils.serialize(readBlockRequest.build()));
				ReadBlockResponse readBlockResponse = (ReadBlockResponse) Utils.deserialize(rBlockResponse);
				int readBlockStatus = readBlockResponse.getStatus();
				List<ByteString> data = readBlockResponse.getDataList();

				for (ByteString str : data) {
					fileContent += str.toStringUtf8();
					System.out.println(str.toStringUtf8());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		String mapOutput = new MapNode().map(fileContent);
		System.out.println(mapOutput);

		// write to file
		String fileName = "job_" + jobId + "_map_" + taskId;
		client.put_file(fileName, fileContent);
	}

	private static void doReduceTask(ReducerTaskInfo reducerTaskInfo) {
		int jobId = reducerTaskInfo.getJobId();
		int taskId = reducerTaskInfo.getTaskId();
		String reduceName = reducerTaskInfo.getReducerName();
		List<String> mapOutputFiles = reducerTaskInfo.getMapOutputFilesList();
		String outputFile = reducerTaskInfo.getOutputFile();
		String fileContent = "";
		for (String fileName : mapOutputFiles) {
			fileContent += client.get_file(fileName);
		}
		String mapOutput = new ReduceNode().reduce(fileContent);
		String outputFileName = outputFile + "_" + jobId + "_" + taskId;
		client.put_file(outputFileName, fileContent); 
	}

	private static void addMapTaskStatus() {

	}

	private static void addReduceTaskStatus() {
		
	}
}