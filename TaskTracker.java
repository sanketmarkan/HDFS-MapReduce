import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;
import java.io.*;
import java.net.*;
import java.lang.reflect.*;

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
			Registry registry = LocateRegistry.getRegistry(Constants.jtIp,Constants.jtPort);
			jobTracker = (IJobTracker) registry.lookup("jobtracker");
			registry = LocateRegistry.getRegistry(Constants.nnIp,Constants.nnPort);
			nameNode = (INameNode) registry.lookup("namenode");
			client = new Client();
		} catch (Exception e) {
			e.printStackTrace();
		}
		// while (true) {
		// 	Scanner in = new Scanner(System.in);
		// 	String line = in.nextLine();
		// 	if (line.equals("beat"))
		// 		heartBeat();
		// }
		new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try{
                        heartBeat();
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // nope
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
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

				Registry registry = LocateRegistry.getRegistry(dnIP,dnPort);
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
		try {
			/*File file = new File(mapName + ".jar");
    		URL url = file.toURI().toURL();

    		System.out.println(url);

			URLClassLoader classLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();

			Method method = URLClassLoader.class.getDeclaredMethod("map", String.class);
		    method.setAccessible(true);
    		
			String mapOutput = (String) method.invoke(classLoader, fileContent);*/

			MapNode myMap = (MapNode) Class.forName(mapName).newInstance();
			String mapOutput = myMap.map(fileContent) + "\n";
			//String mapOutput = new MapNode().map(fileContent) + "\n";
			System.out.println(mapOutput);

			// write to file
			String fileName = "job_" + jobId + "_map_" + taskId;
			client.put_file(fileName, mapOutput);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void doReduceTask(ReducerTaskInfo reducerTaskInfo) {
		int jobId = reducerTaskInfo.getJobId();
		int taskId = reducerTaskInfo.getTaskId();
		String reduceName = reducerTaskInfo.getReducerName();
		List<String> mapOutputFiles = reducerTaskInfo.getMapOutputFilesList();
		String outputFile = reducerTaskInfo.getOutputFile();
		String fileContent = "";
		String mapOutput = "";
		for (String fileName : mapOutputFiles) {
			fileContent = client.get_file(fileName);
			for (String part : fileContent.split("\n")) {
				try {
					ReduceNode myReduce = (ReduceNode) Class.forName(reduceName).newInstance();
					String output = myReduce.reduce(fileContent);
					mapOutput += output;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		String outputFileName = outputFile + "_" + jobId + "_" + taskId;
		client.put_file(outputFileName, mapOutput); 
	}

	private static void addMapTaskStatus() {

	}

	private static void addReduceTaskStatus() {
		
	}
}