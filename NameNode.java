import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

//import dsproject.*;
// import Protobuf.HDFS.DataNodeLocation;
// import Protobuf.HDFS.ListFilesResponse;

import Protobuf.HDFS.*;
import Utils.*;
import INameNode.*;

public class NameNode implements INameNode {
	private static HashMap<String, Integer> fileToInt = new HashMap<>();
	private static HashMap<Integer, ArrayList<Integer>> blockList = new HashMap<>();
	private static HashMap<String, ArrayList<String>> filesDir = new HashMap<>();
	private static HashMap<Integer, ArrayList<DataNodeLocation>> blockLocation = new HashMap<>();
	private static int fileCounter = 1;
	private static int blockCounter = 1;

	public static void main(String args[]){
		init();
	}

    public static void init() {
    	try {
            NameNode obj = new NameNode();
            INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("namenode", stub);
            System.out.println("Namenode ready");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		OpenFileRequest openFileRequest = (OpenFileRequest) Utils.deserialize(inp);
		String fileName = openFileRequest.getFileName();
		boolean forRead = openFileRequest.getForRead();

		System.out.println(fileName);
		System.out.println(forRead);
		
		OpenFileResponse.Builder openFileResponse = OpenFileResponse.newBuilder();
		openFileResponse.setStatus(1);
		openFileResponse.setHandle(1);
		
		boolean isFileExist = true;
		if (fileToInt.get(fileName) == null)
			isFileExist = false;
		
		if (forRead) {
			if (isFileExist) {
				for(int a : blockList.get((fileToInt.get(fileName))))
					openFileResponse.addBlockNums(a);
			} else {
				// ERROR: No such file
				System.out.println("No such file exists");
			}
			return Utils.serialize(openFileResponse.build());
		} else {
			if (isFileExist) {
				// no update operations
				int fileId = fileToInt.get(fileName, blockCounter);
				DataNodeLocation dataNodeLocation = (DataNodeLocation) Utils.deserialize(assignBlock(fileId));
				openFileResponse.addBlockNums(blockCounter);
				blockCounter++;
			} else {
				fileToInt.put(fileName, fileCounter);
				fileCounter++;
			}
			return Utils.serialize(openFileResponse.build());
		}
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		String fileName = new String(inp);
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		BlockLocationRequest blockLocationRequest = (BlockLocationRequest) Utils.deserialize(inp);
		BlockLocationResponse.Builder blockLocationResponse = BlockLocationResponse.newBuilder();

		List<Integer> blockListRequested = blockLocationRequest.getBlockNumsList();
		for (Integer block: blockListRequested) {
			BlockLocations.Builder blockLocations = BlockLocations.newBuilder();
			blockLocations.setBlockNumber(block);
			ArrayList<DataNodeLocation> dataNodeLocations = blockLocation.get(block);
			for (DataNodeLocation location: dataNodeLocations)
				blockLocations.addLocations(location);
			blockLocationResponse.addBlockLocations(blockLocations);
		}
		blockLocationResponse.setStatus(1);
		return Utils.serialize(blockLocationResponse);
	}

	@Override
	public byte[] assignBlock(int fileId, int blockId) {
		// insert in blockList
		ArrayList<Integer> fileBlockList = blockList.get(fileId);
		fileBlockList.add(blockId);
		blockList.put(blockId, fileBlockList);

		// insert in blockLocation
		ArrayList<DataNodeLocation> fileBlockLocations = blockLocation.get(blockId);
		DataNodeLocation.Builder dataNodeLocation = DataNodeLocation.newBuilder();
		dataNodeLocation.setIp('10.1.1.1');
		dataNodeLocation.setPort('80');
		fileBlockLocations.add(dataNodeLocation);
		blockLocation.put(blockId, fileBlockLocations);
		
		return Utils.serialize(dataNodeLocation.build());
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		ListFilesRequest directory = (ListFilesRequest) Utils.deserialize(inp);
		if(directory.hasDirName()){
			ArrayList<String> list = filesDir.get(directory.getDirName());
			return ListFilesResponse.newBuilder().setStatus(1).addAllFileNames(list).build().toByteArray();
		}
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

	public void test() {
		System.out.println("lkasdf");
	}
}
