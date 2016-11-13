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
	private static HashMap<Integer, Integer> handleList = new HashMap<>();
	private static int fileCounter = 1;
	private static int blockCounter = 1;
	private static int handleCounter = 1;

	private static final int STATUS_OK = 1;
	private static final int STATUS_NOT_OK = 0;

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
		
		OpenFileResponse.Builder openFileResponse = OpenFileResponse.newBuilder();
		
		
		boolean isFileExist = true;
		if (fileToInt.get(fileName) == null)
			isFileExist = false;
		
		System.out.println(isFileExist);
		if (forRead) {
			if (isFileExist) {
				for(int a : blockList.get((fileToInt.get(fileName))))
					openFileResponse.addBlockNums(a);
				openFileResponse.setStatus(STATUS_OK);
			} else {
				// ERROR: No such file
				openFileResponse.setStatus(STATUS_NOT_OK);
				System.out.println("No such file exists");
			}
			return Utils.serialize(openFileResponse.build());
		} else {
			if (isFileExist) {
				// No updates allowed
				System.out.println("No updated allowed");
				openFileResponse.setStatus(STATUS_NOT_OK);
			} else {
				System.out.println("Creating file....");	
				fileToInt.put(fileName, fileCounter);
				int fileId = fileCounter;
				int handle = handleCounter;
				handleCounter++;
				fileCounter++;
				handleList.put(handle, fileId);
				openFileResponse.setStatus(STATUS_OK);
				openFileResponse.setHandle(handle);
			}
			return Utils.serialize(openFileResponse.build());
		}
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		CloseFileRequest closeFileRequest = (CloseFileRequest) Utils.deserialize(inp);
		int handle = closeFileRequest.getHandle();

		CloseFileResponse.Builder closeFileResponse = CloseFileResponse.newBuilder();
		try {
			handleList.remove(handle);
			closeFileResponse.setStatus(STATUS_OK);
		} catch (Exception e) {
			closeFileResponse.setStatus(STATUS_NOT_OK);
		}
		return Utils.serialize(closeFileResponse.build());
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
		blockLocationResponse.setStatus(STATUS_OK);
		return Utils.serialize(blockLocationResponse.build());
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		AssignBlockRequest assignBlockRequest = (AssignBlockRequest) Utils.deserialize(inp);
		int handle = assignBlockRequest.getHandle();
		if (handleList.get(handle) == null) {
			AssignBlockResponse.Builder assignBlockResponse = AssignBlockResponse.newBuilder();
			assignBlockResponse.setStatus(STATUS_NOT_OK);
			return Utils.serialize(assignBlockResponse.build());
		}

		int fileId = handleList.get(handle);
		int blockId = blockCounter;
		System.out.println(fileId+" "+handle);
		blockCounter++;
		// insert in blockList
		ArrayList<Integer> fileBlockList = blockList.get(fileId);
		if (fileBlockList == null)
			fileBlockList = new ArrayList<Integer>();
		fileBlockList.add(blockId);
		blockList.put(fileId, fileBlockList);


		// insert in blockLocation
		ArrayList<DataNodeLocation> fileBlockLocations = blockLocation.get(blockId);
		DataNodeLocation.Builder dataNodeLocation = DataNodeLocation.newBuilder();
		dataNodeLocation.setIp("10.1.1.1");
		dataNodeLocation.setPort(80);
		if (fileBlockLocations == null)
			fileBlockLocations = new ArrayList<DataNodeLocation>();
		fileBlockLocations.add(dataNodeLocation.build());
		blockLocation.put(blockId, fileBlockLocations);

		// Building block location 
		BlockLocations.Builder blockLocations = BlockLocations.newBuilder();
		blockLocations.setBlockNumber(blockId);
		blockLocations.addLocations(dataNodeLocation.build());

		AssignBlockResponse.Builder assignBlockResponse = AssignBlockResponse.newBuilder();
		assignBlockResponse.setStatus(STATUS_OK);
		assignBlockResponse.setNewBlock(blockLocations.build());

		return Utils.serialize(assignBlockResponse.build());
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
	public byte[] heartBeat() throws RemoteException {
		return null;
	}

	public void test() {
		System.out.println(fileCounter);
		System.out.println(blockCounter);
		System.out.println("Log");
		System.out.println("------------------");
		for (String fileName: fileToInt.keySet()) {
            int value = fileToInt.get(fileName);
            System.out.println("File: " + fileName);
			System.out.println("Block list:");
			
            ArrayList<Integer> blocks = blockList.get(value);
            for (Integer block : blocks)
            	System.out.println(block);
            System.out.println("---------"); 
		}

		for (Integer i : handleList.keySet()) {
			Integer value = handleList.get(i);
			System.out.println(i + " " + value);
		}
	}
}
