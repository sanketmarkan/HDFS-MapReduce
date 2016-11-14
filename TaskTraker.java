import java.rmi.RemoteException;
import IJobTracker.*;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;
import java.io.*;

import Utils.*;
import MapReduceProto.MapReduce.*;

public class TaskTracker {
	private static IJobTracker jobTracker;
	private static final int taskTrackerId;
	private static int numMapSlotsFree = 1;
	private static int numReduceSlotsFree = 0;

	private static final int STATUS_OK = 1;
	private static final int STATUS_NOT_OK = 0;

	private static final int JOB_FINISH = 2;
	private static final int JOB_STARTED = 1;
	private static final int JOB_WAIT = 0;
	
	public static void main(String args[]) {
		taskTrackerId = 1;
		Registry registry = LocateRegistry.getRegistry();
		jobTracker = (IJobTracker) registry.lookup("jobtracker");
	}

	private void heartBeat() {
		HeartBeatRequest.Builder heartBeatRequest = HeartBeatRequest.newBuilder();
		heartBeatRequest.setTaskTrackerId(taskTrackerId);
		heartBeatRequest.setNumMapSlotsFree(numMapSlotsFree);
		heartBeatRequest.setNumReduceSlotsFree(numReduceSlotsFree);


		byte[] hBeatResponse = jobTracker.heartBeat(heartBeatRequest.build());
		HeartBeatResponse heartBeatResponse = (HeartBeatResponse) Utils.deserialize(hBeatResponse);
		if (heartBeatResponse.getStatus() == STATUS_OK){

		} else {
			// error
		}
	}
}