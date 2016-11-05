import java.io.*;
import java.util.*;
import NameNode.*;
import Utils;

public class Client {
	private static NameNode nameNode = new NameNode();

	public static void main(String args[]) {
		String fileName = "";
		
		if (args[0].equals("get")) {
			fileName = args[1];
			OpenFileRequest openFileRequest = OpenFileRequest.newBuilder();
			openFileRequest.setFileName(fileName);
			openFileRequest.setForRead(true);

			Object response = nameNode.openFile(Utils.serialize(openFileRequest));
			
			OpenFileResponse openFileResponse = Utils.deserialize(response);
		} else if (args[0].equals("put")) {
			fileName = args[1];
			OpenFileRequest openFileRequest = OpenFileRequest.newBuilder();
			openFileRequest.setFileName(fileName);
			openFileRequest.setForRead(false);

			Object oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest));
			
			OpenFileResponse openFileResponse = Utils.deserialize(oFileRespose);
			ArrayList<Integer> blockList = openFileResponse.getBlockNumsList();

			BlockLocationRequest blockLocationRequest = BlockLocationRequest.newBuilder();
			for (Integer block : blockList) {
				blockLocationRequest.setBlockNums(block);
			}

			Object bLocationResponse = nameNode.getBlockLocations(Utils.serialize(blockLocationRequest))
			BlockLocationResponse blockLocationResponse = Utils.deserialize(bLocationResponse);

			int status = blockLocationResponse.getStatus();
			if (status == 1) {
				ArrayList<BlockLocations> blockLocationList =  blockLocationResponse.getBlockLocationsList();
				for (BlockLocations location : blockLocationList) {
					DataNodeLocation dataNodeLocation = location.getLocation();

					DataNode dataNode;
					ArrayList<byte[]> dataBlocks = getDataBlocks(fileName);
					for (byte[] dataBlock : dataBlocks) {
						WriteBlockRequest writeBlockRequest = WriteBlockRequest.newBuilder();
						writeBlockRequest.setBlockLocations(location);
						writeBlockRequest.setData(dataBlock);

						Object wResponse = dataNode.writeBlock(Utils.serialize(writeBlockRequest));
						WriteBlockResponse writeBlockResponse = Utils.deserialize(wResponse);
					}
				}
			} else {
				// error
			}
		} else if (args[0].equals("list")) {
			
		}
		System.out.println(fileName);	
	}

	public static ArrayList<byte[]> getDataBlocks(String fileName) {
		String content = "This is content";
		return null;
	}
}
