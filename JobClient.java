import java.io.*;
import java.util.*;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

import MapReduceProto.MapReduce.*;
import Utils.*;
import IJobTracker.*;

public class JobClient {
	public static void main(String args[]) {
		if(args.length < 0){
			System.out.println("JobClient should be invoked as 'JobClient <mapName> <reducerName> <inputFile in HDFS> <outputFile in HDFS> <numReducers>'");
		}

		try {
			JobSubmitRequest.Builder request = JobSubmitRequest.newBuilder();
			request.setMapName(args[0]);
			request.setReducerName(args[1]);
			request.setInputFile(args[2]);
			request.setOutputFile(args[3]);
			request.setNumReduceTasks(Integer.parseInt(args[4]));

	        Registry registry = LocateRegistry.getRegistry();
	        IJobTracker stub = (IJobTracker) registry.lookup("jobTracker");
	        
	        byte[] responseByte = stub.jobSubmit(Utils.serialize(request));
	        JobSubmitResponse response = (JobSubmitResponse) Utils.deserialize(responseByte);

	        if (response.getStatus() == 1){
	        	int jobId = response.getJobId();
	        	trackJob(jobId);
	        } else {
	        	System.out.println("TRY AGAIN!! SOME ERROR OCCURED!");
	        }
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}

	public static void trackJob(int jobId) {
		try {
			JobStatusRequest.Builder request = JobStatusRequest.newBuilder();
			request.setJobId(jobId);

	        Registry registry = LocateRegistry.getRegistry();
	        IJobTracker stub = (IJobTracker) registry.lookup("jobTracker");
	        while(true) {
		        byte[] responseByte = stub.getJobStatus(Utils.serialize(request));
		        JobStatusResponse response = (JobStatusResponse) Utils.deserialize(responseByte);

		        if (response.getStatus() == 1){
					if(response.getJobDone()){
						System.out.println("JOB DONE!!");
						return;
					}
					System.out.println("Total Map Tasks : "+response.getTotalMapTasks());
					System.out.println("Total Map Tasks Started: "+response.getNumMapTasksStarted());
					System.out.println("Total Reduce Tasks : "+response.getTotalReduceTasks());
					System.out.println("Total Reduce Tasks Started : "+response.getNumReduceTasksStarted());
		        }
		    }
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}
}