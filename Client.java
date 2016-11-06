import java.io.*;
import java.util.*;

import Protobuf.HDFS.*;
import Utils.*;

public class Client {
	// Get NameNode from rmiregistry, not with preceding line
	private static NameNode nameNode = new NameNode();

	public static void main(String args[]) {
		/*
		if (args[0].equals("get")) {
			get_file(args[1]);
		} else if (args[0].equals("put")) {
			put_file(args[1]);
		} else if (args[0].equals("list")) {
			
		} else if (args[0].equals("debug")) {
			debug();
		}*/
		Scanner in = new Scanner(System.in);
		String command = "";
		while (true) {
			command = in.nextLine();
			if (command.equals("get")) {
				get_file("file");
			} else if (command.equals("put")) {
				put_file("file");
			} else if (command.equals("list")) {

			} else if (command.equals("debug")) {
				debug();
			}
		}
	}

	private static void get_file(String fileName) {
		System.out.println(fileName);
		OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
		openFileRequest.setFileName(fileName);
		openFileRequest.setForRead(true);

		try {
			byte[] oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest.build()));
			OpenFileResponse openFileResponse = (OpenFileResponse) Utils.deserialize(oFileRespose);
			int status = openFileResponse.getStatus();
			int handle = openFileResponse.getHandle();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void put_file(String fileName) {
		OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
		openFileRequest.setFileName(fileName);
		openFileRequest.setForRead(false);
		try {
			byte[] oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest.build()));
			OpenFileResponse openFileResponse = (OpenFileResponse) Utils.deserialize(oFileRespose);
			List<Integer> blockList = openFileResponse.getBlockNumsList();
			int status = openFileResponse.getStatus();
			int handle = openFileResponse.getHandle();

			System.out.println(status);
			System.out.println(handle);
			nameNode.test();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*
		BlockLocationRequest.Builder blockLocationRequest = BlockLocationRequest.newBuilder();
		for (Integer block : blockList) {
			blockLocationRequest.addBlockNums(block);
		}

		byte[] bLocationResponse = nameNode.getBlockLocations(Utils.serialize(blockLocationRequest));
		BlockLocationResponse blockLocationResponse = (BlockLocationResponse) Utils.deserialize(bLocationResponse);

		int status = blockLocationResponse.getStatus();
		if (status == 1) {
			List<BlockLocations> blockLocationList =  blockLocationResponse.getBlockLocationsList();
			for (BlockLocations location : blockLocationList) {
				List<DataNodeLocation> dataNodeLocation = location.getLocationsList();

				DataNode dataNode;
				ArrayList<byte[]> dataBlocks = getDataBlocks(fileName);
				for (byte[] dataBlock : dataBlocks) {
					WriteBlockRequest.Builder writeBlockRequest = WriteBlockRequest.newBuilder();
					writeBlockRequest.setBlockInfo(location);
					writeBlockRequest.addData(dataBlock);

					byte[] wResponse = dataNode.writeBlock(Utils.serialize(writeBlockRequest));
					WriteBlockResponse writeBlockResponse = (WriteBlockResponse) Utils.deserialize(wResponse);
				}
			}
		} else {
			// error
		}
		*/
	}

	private static void list_files() {

	}

	private static void debug() {
		nameNode.test();
	}

	public static ArrayList<byte[]> getDataBlocks(String fileName) {
		String content = "This is content";
		return null;
	}
}
