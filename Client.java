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
		String line = "";
		String command = "";
		String fileName = "";
		while (true) {
			line = in.nextLine();
			try {
				String[] arguments = line.trim().split("\\s+");
				command = arguments[0];
				fileName = arguments[1];
			} catch (Exception e) {
				if (command.equals("get") || command.equals("put")) {
					System.out.println("<command> <filename>");
					continue;
				}
			}

			if (command.equals("get")) {
				get_file(fileName);
			} else if (command.equals("put")) {
				put_file(fileName);
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
			List<Integer> blockList = openFileResponse.getBlockNumsList();
			for (Integer block : blockList) {
				System.out.println(block);
			}
			int status = openFileResponse.getStatus();
			int handle = openFileResponse.getHandle();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void put_file(String fileName) {
		int status;
		int fileHandle = -1;

		System.out.println("Opening file...");
		OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
		openFileRequest.setFileName(fileName);
		openFileRequest.setForRead(false);
		try {
			byte[] oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest.build()));
			OpenFileResponse openFileResponse = (OpenFileResponse) Utils.deserialize(oFileRespose);
			List<Integer> blockList = openFileResponse.getBlockNumsList();
			status = openFileResponse.getStatus();
			fileHandle = openFileResponse.getHandle();

			System.out.println("file open status: " + status);
			System.out.println("file handle: " + fileHandle);
			//nameNode.test();
			for (int i=0; i<3; i++) {
				AssignBlockRequest.Builder assignBlockRequest = AssignBlockRequest.newBuilder();
				assignBlockRequest.setHandle(fileHandle);

				byte[] aBlockResponse = nameNode.assignBlock(Utils.serialize(assignBlockRequest.build()));
				AssignBlockResponse assignBlockResponse = (AssignBlockResponse) Utils.deserialize(aBlockResponse);

				int assignBlockStatus = assignBlockResponse.getStatus();
				BlockLocations blockLocations = assignBlockResponse.getNewBlock();
				System.out.println("block assign status: " + assignBlockStatus);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			if (fileHandle != -1) {
				System.out.println("Closing file...");
				CloseFileRequest.Builder closeFileRequest = CloseFileRequest.newBuilder();
				closeFileRequest.setHandle(fileHandle);

				byte[] cFileRequest = nameNode.closeFile(Utils.serialize(closeFileRequest.build()));

				CloseFileResponse closeFileResponse = (CloseFileResponse) Utils.deserialize(cFileRequest);
				int closeStatus = closeFileResponse.getStatus();
				System.out.println("close file status: " + closeStatus);
			}
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
