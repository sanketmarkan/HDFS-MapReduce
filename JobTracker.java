import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;
import java.io.*;

import IJobTracker.*;
import INameNode.*;

import Utils.*;
import com.google.protobuf.ByteString;

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
	private static HashMap<Integer, List<Protobuf.HDFS.BlockLocations>> jobBlockList = new HashMap<>();

	// Map task of a Job
	private static HashMap<Integer, ArrayList<MapTaskInfo>> jobToTasks = new HashMap<>();	
	private static HashMap<Integer, Integer> taskStatusList = new HashMap<>(); // map task status

	// Reduce task of a Job
	private static HashMap<Integer, ArrayList<ReducerTaskInfo>> jobToReduceTask = new HashMap<>();	
	private static HashMap<Integer, Integer> reduceTaskStatusList = new HashMap<>(); // reduce task status

	// reducer's list
	private static HashMap<Integer, Set<Integer>> reducerList = new HashMap<>();
	private static HashMap<Integer, Integer> reducerAllowed = new HashMap<>();

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

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try{
                        updateJobStatus();
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

	public byte[] jobSubmit(byte[] inp) {
		JobSubmitRequest jobSubmitRequest = (JobSubmitRequest) Utils.deserialize(inp);
		int jobId = jobCounter;
		jobCounter++;

		String mapName, reducerName, inputFile, outputFile;
		int numReduceTasks;
		mapName = jobSubmitRequest.getMapName();
		reducerName = jobSubmitRequest.getReducerName();
		inputFile = jobSubmitRequest.getInputFile();
		outputFile = jobSubmitRequest.getOutputFile();
		numReduceTasks = jobSubmitRequest.getNumReduceTasks();

		reducerAllowed.put(jobId, numReduceTasks);
		
		OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
		openFileRequest.setFileName(inputFile);
		openFileRequest.setForRead(true);
		
		JobSubmitResponse.Builder jobSubmitResponse = JobSubmitResponse.newBuilder();
		jobSubmitResponse.setStatus(STATUS_OK);
		jobSubmitResponse.setJobId(jobId);

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
			List<Protobuf.HDFS.BlockLocations> blockLocationList = blockLocationResponse.getBlockLocationsList();

			// breaking job into map tasks
			ArrayList<MapTaskInfo> tasks = new ArrayList<MapTaskInfo>();
			for(Protobuf.HDFS.BlockLocations blockLocation :blockLocationList) {
				MapTaskInfo.Builder mapTaskInfo = MapTaskInfo.newBuilder();
				
				int taskId = taskCounter;
				taskStatusList.put(taskId, JOB_WAIT);
				taskCounter++;

				mapTaskInfo.setMapName(mapName);
				mapTaskInfo.setTaskId(taskId);
				mapTaskInfo.setJobId(jobId);
				mapTaskInfo.addInputBlocks(adapter(blockLocation));
				tasks.add(mapTaskInfo.build());
			}
			jobToTasks.put(jobId, tasks);
			jobQueue.add(jobId);
			jobStatusList.put(jobId, JOB_WAIT);

			// appending reduce tasks into queue
			ArrayList<ReducerTaskInfo> reduceTasks = new ArrayList<ReducerTaskInfo>();
			for (MapTaskInfo mapTask : tasks) {
				ReducerTaskInfo.Builder reducerTaskInfo = ReducerTaskInfo.newBuilder();

				int mapTaskId = mapTask.getTaskId();
				int reduceTaskId = taskCounter;
				taskCounter++;

				String mapOutputFile = "job_" + jobId + "_map_" + mapTaskId;
				reducerTaskInfo.setJobId(jobId);
				reducerTaskInfo.setTaskId(reduceTaskId);
				reducerTaskInfo.addMapOutputFiles(mapOutputFile);

				reducerTaskInfo.setReducerName(reducerName);
				reducerTaskInfo.setOutputFile(outputFile);

				reduceTasks.add(reducerTaskInfo.build());
				reduceTaskStatusList.put(reduceTaskId, JOB_WAIT);
			}
			jobToReduceTask.put(jobId, reduceTasks);

		} catch(Exception e) {
			jobSubmitResponse.setStatus(STATUS_NOT_OK);
			e.printStackTrace();
		}
		System.out.println("Job submitted: " + jobId);
		return Utils.serialize(jobSubmitResponse.build());
	}

	public byte[] getJobStatus(byte[] inp) {
		JobStatusRequest jobStatusRequest = (JobStatusRequest) Utils.deserialize(inp);
		int jobId = jobStatusRequest.getJobId();

		System.out.println("Job Status Query: " + jobId);
		printJobStatus();
		JobStatusResponse.Builder jobStatusResponse = JobStatusResponse.newBuilder();
		if (jobId < jobCounter && jobId > 0) {
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

		updateMapTaskStatus(heartBeatRequest.getMapStatusList());
		updateReduceTaskStatus(heartBeatRequest.getReduceStatusList());


		HeartBeatResponse.Builder heartBeatResponse = HeartBeatResponse.newBuilder();

		if (numMapSlotsFree > 0) {	
			MapTaskInfo mapTaskInfo = assignJobToMaps();
			if (mapTaskInfo != null)
				heartBeatResponse.addMapTasks(mapTaskInfo);
		}
		if (numReduceSlotsFree > 0) {
			ReducerTaskInfo reduceTaskInfo = assignJobToReduce(taskTrackerId);
			if (reduceTaskInfo != null) {
				heartBeatResponse.addReduceTasks(reduceTaskInfo);
			}
		}
		heartBeatResponse.setStatus(STATUS_OK);
		
		return Utils.serialize(heartBeatResponse.build());
	}

	// Break job into map & reduce tasks just after job submitted, not at the time when TT calls
	private MapTaskInfo assignJobToMaps() {
		JobSubmitRequest jobSubmitRequest;
		String mapName, reducerName, inputFile, outputFile;
		int numReduceTasks;
		List<Protobuf.HDFS.BlockLocations> blockLocations;
		if (jobQueue.size() != 0) {
			int jobId = jobQueue.remove();

			ArrayList<MapTaskInfo> tasks = jobToTasks.get(jobId);
			if(tasks.size()>0){
				MapTaskInfo to_give_task = tasks.get(0);
				ArrayList<MapTaskInfo> newList = new ArrayList<MapTaskInfo>();
				for(int i=1;i<tasks.size();i++){
					newList.add(tasks.get(i));
				}
				tasks = newList;
				if(tasks.size()>0){
					jobToTasks.put(jobId, tasks);
					jobQueue.add(jobId);
				} else {
					jobToTasks.remove(jobId);
				}
				return to_give_task;
			}

        }
        return null;
	}

	private ReducerTaskInfo assignJobToReduce(int taskTrackerId) {
		for (Integer jobId : jobStatusList.keySet()) {
			int status = jobStatusList.get(jobId);

			Set<Integer> reducers = reducerList.get(jobId);
			int allowed = reducerAllowed.get(jobId);
			if (allowed == reducers.size()) {
				int done = 0;
				for (int i : reducers) {
					if (taskTrackerId == i) {
						done = 1;
						break;
					}
				}
				if(done==0) continue;
			} else if (reducers.size() < allowed) {
				reducers.add(taskTrackerId);
				reducerList.put(jobId, reducers);
			} else
				continue;

			if (status == JOB_MAP_DONE) {
				ArrayList<ReducerTaskInfo> reduceTasks = jobToReduceTask.get(jobId);
				if(reduceTasks.size()>0){
					ReducerTaskInfo to_give_task = reduceTasks.get(0);
					ArrayList<ReducerTaskInfo> newList = new ArrayList<ReducerTaskInfo>();
					for(int i=1;i<reduceTasks.size();i++){
						newList.add(reduceTasks.get(i));
					}
					reduceTasks = newList;
					if(reduceTasks.size()>0){
						jobToReduceTask.put(jobId, reduceTasks);
					} else {
						jobToReduceTask.remove(jobId);
					}
					
					return to_give_task;
				}
			}
		}
		return null;
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

	private void updateMapTaskStatus(List<MapTaskStatus> statusList) {
		for (MapTaskStatus status : statusList) {
			int jobId = status.getJobId();
			int taskId = status.getTaskId();
			if (status.getTaskCompleted()) {
				taskStatusList.put(taskId, JOB_FINISH);
			}
		}	
	}

	private void updateReduceTaskStatus(List<ReduceTaskStatus> statusList) {
		for (ReduceTaskStatus status : statusList) {
			int jobId = status.getJobId();
			int taskId = status.getTaskId();
			if (status.getTaskCompleted()) {
				reduceTaskStatusList.put(taskId, JOB_FINISH);
			}
		}
	}

	private static void updateJobStatus() {
		// update job status
		for (Integer jobId : jobStatusList.keySet()) {
			ArrayList<MapTaskInfo> mapTasks = jobToTasks.get(jobId);
			boolean jobMapDone = true;
			if (mapTasks != null) {
				for (MapTaskInfo task : mapTasks) {
					if (taskStatusList.get(task.getTaskId()) != JOB_FINISH) {
						jobMapDone = false;
						break;
					}
				}
			}

			ArrayList<ReducerTaskInfo> reduceTasks = jobToReduceTask.get(jobId);
			boolean jobReduceDone = true;
			if (reduceTasks != null) {
				for (ReducerTaskInfo task : reduceTasks) {
					if (reduceTaskStatusList.get(task.getTaskId()) != JOB_FINISH) {
						jobReduceDone = false;
						break;
					}
				}
			}
			if (jobMapDone && jobReduceDone)
				jobStatusList.put(jobId, JOB_FINISH);
			else if (jobMapDone)
				jobStatusList.put(jobId, JOB_MAP_DONE);
		}
	}

	public void printJobStatus() {
		for (Integer jobId : jobStatusList.keySet()) {
			System.out.println("Job: " + jobId + " Status :" + jobStatusList.get(jobId));
			ArrayList<MapTaskInfo> mapTasks = jobToTasks.get(jobId);
			if (mapTasks != null) {
				System.out.println("Map");
				for (MapTaskInfo task : mapTasks) {
					System.out.println(task + ":" + taskStatusList.get(task.getTaskId()));
				}
			}

			ArrayList<ReducerTaskInfo> reduceTasks = jobToReduceTask.get(jobId);
			if (reduceTasks != null) {
				System.out.println("Reduce");
				for (ReducerTaskInfo task : reduceTasks) {
					System.out.println(task + ":" + reduceTaskStatusList.get(task.getTaskId()));
				}
			}
		}	
	}

} 
