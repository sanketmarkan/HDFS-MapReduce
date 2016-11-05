import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

import Protobuf.HDFS.DataNodeLocation;
import Protobuf.HDFS.ListFilesResponse;

import INameNode.*;

public class NameNode implements INameNode {

	private static HashMap<String, Integer> fileToInt = new HashMap<>();
	private static HashMap<Integer, ArrayList<Integer>> blockList = new HashMap<>();
	private static HashMap<Integer, ArrayList<DataNodeLocation>> blockLocation = new HashMap<>();
	
	public static void main(String[] args) {

	}

	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		OpenFileRequest openFileRequest = (OpenFileRequest) deserialize(inp);
		String fileName = openFileRequest.getFileName();
		boolean forRead = openFileRequest.getForRead();
		
		OpenFileResponse openFileResponse = OpenFileResponse.newBuilder();
		openFileResponse.setStatus(1);
		openFileResponse.setHandle(1);
		openFileResponse.setBlockNums(1);
		openFileResponse.setBlockNums(2);
		openFileResponse.setBlockNums(3);
		if (forRead) {
			return serialize(openFileResponse);
		} else {
			return serialize(openFileResponse);
		}
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		String fileName = new String(inp);
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		BlockLocationRequest blockLocationRequest = (BlockLocationRequest) deserialize(inp);
		BlockLocationResponse blockLocationResponse = BlockLocationResponse.newBuilder();

		ArrayList<Integer> blockListRequested = blockLocationRequest.getBlockNumsList();
		for (Integer block: blockListRequested) {
			BlockLocations blockLocations = BlockLocations.newBuilder();
			blockLocation.setBlockNumber(block);
			ArrayList<DataNodeLocation> dataNodeLocations = blockLocation.get(block);
			BlockLocations.setDataNodeLocation(dataNodeLocations);
			
			blockLocationResponse.setBlockLocations(blockLocations);
		}
		blockLocationResponse.setStatus(1);
		return serialize(blockLocationResponse);
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		return null;
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		// int a = ListFilesResponse.newBuilder();
		return ListFilesResponse.newBuilder().setStatus(1).addAllFileNames(fileToInt.keySet()).build().toByteArray();
	}

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		return null;
	}


	public static byte[] serialize(Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            byte[] bArray = bos.toByteArray();
            return bArray;
        } catch (Exception e) {
            return null;
        }
    }

    public static Object deserialize(byte[] response) {
        ByteArrayInputStream bis = new ByteArrayInputStream(response);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (Exception e) {
            return null;
        }
    }

}
