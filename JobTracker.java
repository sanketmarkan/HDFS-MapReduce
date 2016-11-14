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

	// Map task of a Job
	private static HashMap<Integer, ArrayList<Integer>> jobToTasks = new HashMap<>();	
	private static HashMap<Integer, Integer> taskStatusList = new HashMap<>(); // map task status

	// Reduce task of a Job
	private static HashMap<Integer, ArrayList<Integer>> jobToReduceTask = new HashMap<>();	
	private static HashMap<Integer, Integer> reduceTaskStatusList = new HashMap<>(); // reduce task status


	private static final int JOB_FINISH = 2;
	private static final int JOB_MAP_DONE = 1;
	private static final int JOB_WAIT = 0;

	private static final int STATUS_OK = 1;
	private static final int STATUS_NOT_OK = 0;

	private static INameNode nameNode;

	public static void main(String args[]) {
		try {
            JobTracker obj = new JobTracker();
            IJobTracker stub = (IJobTracker) UnicastRemoteObject.exportObject(obj, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("jobtracker", stub);
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
		jobList.put(jobId, jobSubmitRequest);
		
		JobSubmitResponse.Builder jobSubmitResponse = JobSubmitResponse.newBuilder();
		jobSubmitResponse.setStatus(STATUS_OK);
		jobSubmitResponse.setJobId(jobId);

		jobStatusList.put(jobId, JOB_WAIT);
		return Utils.serialize(jobSubmitResponse.build());
	}

	public byte[] getJobStatus(byte[] inp) {
		JobStatusRequest jobStatusRequest = (JobStatusRequest) Utils.deserialize(inp);
		int jobId = jobStatusRequest.getJobId();

		System.out.println("Job Status Query: " + jobId);
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

		updateJobStatus(heartBeatRequest.getMapStatusList());
		HeartBeatResponse.Builder heartBeatResponse = HeartBeatResponse.newBuilder();

		if (numMapSlotsFree > 0) {	
			MapTaskInfo mapTaskInfo = assignJobToMaps();
			heartBeatResponse.addMapTasks(mapTaskInfo);
		}
		if (numReduceSlotsFree > 0) {
			ReducerTaskInfo reduceTaskInfo = assignJobToReduce();
			heartBeatResponse.addReduceTasks(reduceTaskInfo);
		}

		heartBeatResponse.setStatus(STATUS_OK);
		return return Utils.serialize(heartBeatResponse.build());
	}

	private MapTaskInfo assignJobToMaps() {
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
					mapTaskInfo.addInputBlocks(adapter(blockLocation));
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

	private ReducerTaskInfo assignJobToReduce() {
		for (Integer jobId : jobStatusList.keySet()) {
			int status = jobStatusList.get(jobId);
			if (status == JOB_MAP_DONE) {
				ReducerTaskInfo.Builder reducerTaskInfo = ReducerTaskInfo.newBuilder();
				reducerTaskInfo.setJobId(jobId);

				//jobToTasks 

				int taskId = taskCounter; 
				reduceTaskStatusList.put(taskId, JOB_WAIT);
				taskCounter++;
				return reducerTaskInfo.build();
			}
		}
	}

	private BlockLocations adapter(Protobuf.HDFS.BlockLocations location) {
		BlockLocations.Builder blockLoc = BlockLocations.newBuilder();
		blockLoc.setBlockNumber(location.getBlockNumber());

		List<Protobuf.HDFS.DataNodeLocation> dataNodeLocations = location.getLocationsList();
		for (Protobuf.HDFS.DataNodeLocation dataNodeLocation : dataNodeLocations) { 
			DataNodeLocation.Builder dnLocation = DataNodeLocation.newBuilder();
			dnLocation.setIp(dataNodeLocation.getIp());
			dnLocation.setPort(dataNodeLocation.getPort());
			blockLoc.addLocations(dnLocation.build());
		}

		return blockLoc.build();
	}

	/*private void updateMapTaskStatus(List<MapTaskStatus> statusList) {
		int jobId = 0;
		for (MapTaskStatus status : statusList) {
			jobId = status.getJobId();
			int taskId = status.getTaskId();
			if (status.getTaskCompleted()) {
				taskStatusList.put(taskId, JOB_FINISH);
			}
		}	
	}

	private void updateReduceTaskStatus(List<ReduceTaskStatus> statusList) {
		int jobId = 0;
		for (ReduceTaskStatus status : statusList) {
			jobId = status.getJobId();
			int taskId = status.getTaskId();
			if (status.getTaskCompleted()) {
				reduceTaskStatusList.put(taskId, JOB_FINISH);
			}
		}
	}*/

	private void updateJobStatus(List<Object> statusList) {
		for (Object status : statusList) {
			int jobId = status.getJobId();
			int taskId = status.getTaskId();
			// update task status
			if (status InstanceOf MapTaskStatus) {
				if (((MapTaskStatus)status).getTaskCompleted()) {
					taskStatusList.put(taskId, JOB_FINISH);
				}
			} else {
				if (((ReduceTaskStatus)status).getTaskCompleted()) {
					reduceTaskStatusList.put(taskId, JOB_FINISH);
				}
			}
			
			// update job status
			ArrayList<Integer> mapTasks = jobToTasks.get(jobId);
			boolean jobMapDone = true;
			for (Integer task : mapTasks) {
				if (jobToTasks.get(jobToTasks) != JOB_FINISH) {
					jobDone = false;
					break;
				}
			}

			ArrayList<Integer> reduceTasks = jobToReduceTask.get(jobId);
			boolean jobReduceDone = true;
			for (Integer task : reduceTasks) {
				if (jobToReduceTask.get(jobToTasks) != JOB_FINISH) {
					jobDone = false;
					break;
				}
			}
			if (jobMapDone && jobReduceDone)
				jobStatusList.put(jobId, JOB_FINISH);
			else if (jobMapDone)
				jobStatusList.put(jobId, JOB_MAP_DONE);
		}
	}

} 
