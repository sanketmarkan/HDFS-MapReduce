import java.rmi.RemoteException;
import IJobTracker.*;
import java.rmi.server.UnicastRemoteObject;

import Utils.*;
import MapReduceProto.MapReduce.*;

public class JobTracker implements IJobTracker {
	private static int jobCounter = 1;
	private static int taskCounter = 1;

	PriorityQueue<JobSubmitRequest> jobQueue = new PriorityQueue<JobSubmitRequest>();
	private static HashMap<int, status> jobStatusList = new HashMap<>();
	private static HashMap<int, ArrayList<int>> mapJobStatusList = new HashMap<>(); // map to task
	private static HashMap<int, status> taskStatusList = new HashMap<>(); // task status

	private static final int JOB_FINISH = 2;
	private static final int JOB_STARTED = 1;
	private static final int JOB_WAIT = 0;

	private static final int STATUS_OK = 1;
	private static final int STATUS_NOT_OK = 0;

	public byte[] jobSubmit(byte[] inp) {
		JobSubmitRequest jobSubmitRequest = (JobSubmitRequest) Utils.deserialize(inp);
		int jobId = jobCounter;
		jobCounter++;

		jobQueue.insert(jobSubmitRequest);
		
		JobSubmitResponse.Builder jobSubmitResponse = JobSubmitResponse.newBuilder();
		jobSubmitResponse.setStatus(STATUS_OK);
		jobSubmitResponse.setJobId(jobId);

		jobStatusList.put(jobId, JOB_WAIT);
		return Utils.serialize(jobSubmitResponse.build());
	}

	public byte[] getJobStatus(byte[] inp) {
		return null;
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
			return heartBeatResponse.build();
		}
		return null;
	}

	private MapTaskInfo assignJobToMaps(DataNodeLocation location) {
		JobSubmitRequest jobSubmitRequest;
		String mapName, reducerName, inputFile, outputFile;
		int numReduceTasks;
		if (jobQueue.size() != 0) {
			jobSubmitRequest = queue.remove();
			
			mapName = jobSubmitRequest.getMapName();
			reducerName = jobSubmitRequest.getReducerName();
			inputFile = jobSubmitRequest.getInputFile();
			outputFiel = jobSubmitRequest.getOutputFile();
			numReduceTasks = jobSubmitRequest.getNumReduceTasks();

			OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
			openFileRequest.setFileName(fileName);
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
				List<BlockLocations> blockLocations = blockLocationResponse.getBlockLocationsList(); 
				for (BlockLocations location : blockLocations) {
					mapTaskInfo.addInputBlocks(location);
				}
				/*
				String dnIP = location.getIp();
				int dnPort = location.getPort();

				Registry registry = LocateRegistry.getRegistry();
	        	IDataNode taskTracker = (IDataNode) registry.lookup("taskTracker");
				*/
				taskStatusList.put(taskCounter, JOB_WAIT);
				taskCounter++;
				return MapTaskInfo.build();

			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        return null;
	}
} 
