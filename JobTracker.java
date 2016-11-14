import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;
import java.io.*;

import IJobTracker.*;
import INameNode.*;

import Utils.*;

import Protobuf.HDFS.OpenFileRequest;
import Protobuf.HDFS.OpenFileResponse;
import Protobuf.HDFS.BlockLocationRequest;
import Protobuf.HDFS.BlockLocationResponse;

import MapReduceProto.MapReduce.*;

public class JobTracker implements IJobTracker {
	private static int jobCounter = 1;
	private static int taskCounter = 1;

	PriorityQueue<Integer> jobQueue = new PriorityQueue<Integer>();
	private static HashMap<Integer, JobSubmitRequest> jobList = new HashMap<>();
	private static HashMap<Integer, Integer> jobStatusList = new HashMap<>();

	private static HashMap<Integer, ArrayList<Integer>> jobToTasks = new HashMap<>();	
	private static HashMap<Integer, ArrayList<Integer>> mapJobStatusList = new HashMap<>(); // map to task
	private static HashMap<Integer, Integer> taskStatusList = new HashMap<>(); // task status

	private static final int JOB_FINISH = 2;
	private static final int JOB_STARTED = 1;
	private static final int JOB_WAIT = 0;

	private static final int STATUS_OK = 1;
	private static final int STATUS_NOT_OK = 0;

	private static INameNode nameNode;

	public static void main(String args[]) {
		try {
            JobTracker obj = new JobTracker();
            IJobTracker stub = (IJobTracker) UnicastRemoteObject.exportObject(obj, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("jobtraker", stub);
            System.out.println("JobTracker ready");

	        nameNode = (INameNode) registry.lookup("namenode");
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	public byte[] jobSubmit(byte[] inp) {
		JobSubmitRequest jobSubmitRequest = (JobSubmitRequest) Utils.deserialize(inp);
		int jobId = jobCounter;
		jobCounter++;

		jobQueue.add(jobId);
		
		JobSubmitResponse.Builder jobSubmitResponse = JobSubmitResponse.newBuilder();
		jobSubmitResponse.setStatus(STATUS_OK);
		jobSubmitResponse.setJobId(jobId);

		jobStatusList.put(jobId, JOB_WAIT);
		return Utils.serialize(jobSubmitResponse.build());
	}

	public byte[] getJobStatus(byte[] inp) {
		JobStatusRequest jobStatusRequest = (JobStatusRequest) Utils.deserialize(inp);
		int jobId = jobStatusRequest.getJobId();

		JobStatusResponse.Builder jobStatusResponse = JobStatusResponse.newBuilder();
		if (jobId < jobCounter) {
			jobStatusResponse.setStatus(STATUS_OK);
			jobStatusResponse.setJobDone(jobStatusList.get(jobId) == JOB_FINISH);
			jobStatusResponse.setTotalMapTasks(0);
			jobStatusResponse.setNumMapTasksStarted(0);
			jobStatusResponse.setTotalReduceTasks(0);
			jobStatusResponse.setNumReduceTasksStarted(0);			
		} else
			jobStatusResponse.setStatus(STATUS_NOT_OK);
		return Utils.serialize(jobStatusResponse.build());
	}
	
	public byte[] heartBeat(byte[] inp) {
		HeartBeatRequest heartBeatRequest = (HeartBeatRequest) Utils.deserialize(inp);
		int taskTrackerId = heartBeatRequest.getTaskTrackerId();
		int numMapSlotsFree = heartBeatRequest.getNumMapSlotsFree();
		int numReduceSlotsFree = heartBeatRequest.getNumReduceSlotsFree();
		DataNodeLocation location =  heartBeatRequest.getLocations();
		if (numReduceSlotsFree > 0) {
			HeartBeatResponse.Builder heartBeatResponse = HeartBeatResponse.newBuilder();
			MapTaskInfo mapTaskInfo = assignJobToMaps(location);
			heartBeatResponse.addMapTasks(mapTaskInfo);
			heartBeatResponse.setStatus(STATUS_OK);
			return Utils.serialize(heartBeatResponse.build());
		}
		return null;
	}

	private MapTaskInfo assignJobToMaps(DataNodeLocation location) {
		JobSubmitRequest jobSubmitRequest;
		String mapName, reducerName, inputFile, outputFile;
		int numReduceTasks;
		if (jobQueue.size() != 0) {
			int jobId = jobQueue.remove();
			jobSubmitRequest = jobList.get(jobId);
			
			mapName = jobSubmitRequest.getMapName();
			reducerName = jobSubmitRequest.getReducerName();
			inputFile = jobSubmitRequest.getInputFile();
			outputFile = jobSubmitRequest.getOutputFile();
			numReduceTasks = jobSubmitRequest.getNumReduceTasks();

			OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
			openFileRequest.setFileName(inputFile);
			openFileRequest.setForRead(true);

			try {
				byte[] oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest.build()));
				OpenFileResponse openFileResponse = (OpenFileResponse) Utils.deserialize(oFileRespose);
				List<Integer> blockList = openFileResponse.getBlockNumsList();
				BlockLocationRequest.Builder blockLocationRequest = BlockLocationRequest.newBuilder();
				for (Integer block : blockList) {
					blockLocationRequest.addBlockNums(block);
					System.out.println(block);
				}

				byte[] bLocationResponse = nameNode.getBlockLocations(Utils.serialize(blockLocationRequest.build()));
				BlockLocationResponse blockLocationResponse = (BlockLocationResponse) Utils.deserialize(bLocationResponse);
				
				int locationStatus = blockLocationResponse.getStatus();

				MapTaskInfo.Builder mapTaskInfo = MapTaskInfo.newBuilder();
				mapTaskInfo.setMapName(mapName);
				
				List<Protobuf.HDFS.BlockLocations> blockLocations = blockLocationResponse.getBlockLocationsList(); 
				for (Protobuf.HDFS.BlockLocations blockLocation : blockLocations) {
					BlockLocations.Builder blockLoc = BlockLocations.newBuilder();
					blockLoc.setBlockNumber(blockLocation.getBlockNumber());
					blockLoc.setLocations(blockLocation.getLocationsList());
					mapTaskInfo.addInputBlocks(blockLoc.build());
				}
				
				int taskId = taskCounter; 
				taskStatusList.put(taskId, JOB_WAIT);
				taskCounter++;
				ArrayList<Integer> tasks = jobToTasks.get(jobId);
				if (tasks == null)
					tasks = new ArrayList<Integer>();
				tasks.add(taskId);
				jobToTasks.put(jobId, tasks);

				return mapTaskInfo.build();

			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        return null;
	}


} 
