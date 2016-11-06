import java.io.*;
import java.util.*;

import Protobuf.HDFS.*;
import Utils.*;

public class Client {
	private static NameNode nameNode = new NameNode();

	public static void main(String args[]) {
		//nameNode.test();
		String fileName = "";
		if (args[0].equals("get")) {
			fileName = args[1];
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
		} else if (args[0].equals("put")) {
			/*
			fileName = args[1];
			OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
			openFileRequest.setFileName(fileName);
			openFileRequest.setForRead(false);

			byte[] oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest));
			
			OpenFileResponse openFileResponse = (OpenFileResponse) Utils.deserialize(oFileRespose);
			List<Integer> blockList = openFileResponse.getBlockNumsList();

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
		} else if (args[0].equals("list")) {

		}
	}

	public static ArrayList<byte[]> getDataBlocks(String fileName) {
		String content = "This is content";
		return null;
	}
}
