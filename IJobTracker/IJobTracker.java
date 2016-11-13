package IJobTracker;
public interface IJobTracker {
	
	/* JobSubmitResponse jobSubmit(JobSubmitRequest) */
	byte[] jobSubmit(byte[] inp);

	/* JobStatusResponse getJobStatus(JobStatusRequest) */
	byte[] getJobStatus(byte[] inp);
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
	byte[] heartBeat(byte[] inp);
}
