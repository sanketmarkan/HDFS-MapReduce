import java.io.*;
import java.util.*;

import Protobuf.HDFS.*;
import Utils.*;
import IDataNode.*;
import INameNode.*;

import com.google.protobuf.ByteString;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

public class Client {
	private static INameNode nameNode;
	public Client() {
		try {
			Registry registry = LocateRegistry.getRegistry(/*Constants.nnIp,Constants.nnPort*/);
			nameNode = (INameNode) registry.lookup("namenode");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {
		if (nameNode == null) {
			try {
				Registry registry = LocateRegistry.getRegistry(/*Constants.nnIp,Constants.nnPort*/);
				nameNode = (INameNode) registry.lookup("namenode");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
			
		Scanner in = new Scanner(System.in);
		String line = "";
		String command = "";
		String fileName = "";
		String outFile = "";
		while (true) {
			line = in.nextLine();
				String[] arguments = line.trim().split("\\s+");
			try {
				command = arguments[0];
				fileName = arguments[1];
				// outFile = 
			} catch (Exception e) {
				if (command.equals("get") || command.equals("put")) {
					System.out.println("<command> <filename>");
					continue;
				}
			}
			//System.out.println(command);
			//System.out.println(fileName);

			if (command.equals("get")) {
				get_file(fileName);
			} else if (command.equals("put")) {
				outFile = arguments[2];
				String data = getFileContent(fileName);
				put_file(outFile, data);
			} else if (command.equals("list")) {
				list_files();
			} else if (command.equals("debug")) {
				debug();
			}
		}
	}

	public static String get_file(String fileName) {
		String fileContent = "";
		System.out.println(fileName);
		OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
		openFileRequest.setFileName(fileName);
		openFileRequest.setForRead(true);

		try {
			byte[] oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest.build()));
			OpenFileResponse openFileResponse = (OpenFileResponse) Utils.deserialize(oFileRespose);
			List<Integer> blockList = openFileResponse.getBlockNumsList();
			BlockLocationRequest.Builder blockLocationRequest = BlockLocationRequest.newBuilder();
			for (Integer block : blockList) {
				blockLocationRequest.addBlockNums(block);
				System.out.println(block);
			}
			byte[] bLocationResponse = nameNode.getBlockLocations(Utils.serialize(blockLocationRequest.build()));
			BlockLocationResponse blockLocationResponse = (BlockLocationResponse) Utils.deserialize(bLocationResponse);
			
			int locationStatus = blockLocationResponse.getStatus();
			System.out.println("block location status: " + locationStatus);

			List<BlockLocations> blockLocations = blockLocationResponse.getBlockLocationsList(); 
			for (BlockLocations location : blockLocations) {
				List<DataNodeLocation> dataNodeLocationList = location.getLocationsList();
				for(DataNodeLocation dataNodeLocation : dataNodeLocationList) {
					String dnIP = dataNodeLocation.getIp();
					int dnPort = dataNodeLocation.getPort();
					int blockNumber = location.getBlockNumber();

					Registry registry = LocateRegistry.getRegistry(dnIP,dnPort);
		        	IDataNode dataNode = (IDataNode) registry.lookup("datanode");

		        	ReadBlockRequest.Builder readBlockRequest = ReadBlockRequest.newBuilder();
		        	readBlockRequest.setBlockNumber(blockNumber);

		        	byte[] rBlockResponse = dataNode.readBlock(Utils.serialize(readBlockRequest.build()));
					ReadBlockResponse readBlockResponse = (ReadBlockResponse) Utils.deserialize(rBlockResponse);
					int readBlockStatus = readBlockResponse.getStatus();
					if(readBlockStatus == 1) {
						List<ByteString> data = readBlockResponse.getDataList();
						for (ByteString str : data) {
							fileContent += str.toStringUtf8();
							System.out.println(str.toStringUtf8());
						}
						break;
					}
				}
			}
			int status = openFileResponse.getStatus();
			int handle = openFileResponse.getHandle();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileContent;
	}

	public static void put_file(String fileName, String data) {
		//System.out.println(data);
		int status;
		int fileHandle = -1;

		System.out.println("Opening file...");
		OpenFileRequest.Builder openFileRequest = OpenFileRequest.newBuilder();
		openFileRequest.setFileName(fileName);
		openFileRequest.setForRead(false);

		int loop = ((data.length())/(1024)) + 1;
		int lastIdx = 0;
		System.out.println(loop);
		
			try {
				byte[] oFileRespose = nameNode.openFile(Utils.serialize(openFileRequest.build()));
				OpenFileResponse openFileResponse = (OpenFileResponse) Utils.deserialize(oFileRespose);
				List<Integer> blockList = openFileResponse.getBlockNumsList();
				status = openFileResponse.getStatus();
				fileHandle = openFileResponse.getHandle();

				System.out.println("file open status: " + status);
				System.out.println("file handle: " + fileHandle);
				//nameNode.test();
				for (int i=0; i<loop; i++) {
			String sub;
			if (i == loop-1)
				sub = data.substring(lastIdx);
			else
				sub = data.substring(lastIdx, lastIdx+(1024));
			System.out.println(sub);
				AssignBlockRequest.Builder assignBlockRequest = AssignBlockRequest.newBuilder();
				assignBlockRequest.setHandle(fileHandle);

				byte[] aBlockResponse = nameNode.assignBlock(Utils.serialize(assignBlockRequest.build()));
				AssignBlockResponse assignBlockResponse = (AssignBlockResponse) Utils.deserialize(aBlockResponse);

				int assignBlockStatus = assignBlockResponse.getStatus();
				BlockLocations blockLocations = assignBlockResponse.getNewBlock();

				System.out.println("block assign status: " + assignBlockStatus);


				List<DataNodeLocation> dataNodeLocationList = blockLocations.getLocationsList();
				// System.out.println(dataNodeLocationList.)
				

				for(DataNodeLocation dataNodeLocation : dataNodeLocationList) {
					String dnIP = dataNodeLocation.getIp();
					int dnPort = dataNodeLocation.getPort();
					int blockNumber = blockLocations.getBlockNumber();
					Registry registry = LocateRegistry.getRegistry(dnIP,dnPort);
			    	IDataNode dataNode = (IDataNode) registry.lookup("datanode");

			    	WriteBlockRequest.Builder writeBlockRequest = WriteBlockRequest.newBuilder();
					writeBlockRequest.setBlockNumber(blockNumber);
					writeBlockRequest.addData(ByteString.copyFromUtf8(sub));
				

					byte[] wBlockResponse = dataNode.writeBlock(Utils.serialize(writeBlockRequest.build()));
					WriteBlockResponse writeBlockResponse = (WriteBlockResponse) Utils.deserialize(wBlockResponse);
					int writeBlockStatus = writeBlockResponse.getStatus();
					System.out.println("block write status: " + writeBlockStatus);
				}
			}} catch (Exception e) {
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
	}

	private static void list_files() {
		try {
			ListFilesRequest.Builder listFilesRequest = ListFilesRequest.newBuilder();
			// listFilesRequest.setDirName(".");
			byte[] lFilesResponse = nameNode.list(Utils.serialize(listFilesRequest.build()));

			ListFilesResponse listFilesResponse = (ListFilesResponse) Utils.deserialize(lFilesResponse);
			List<String> fileList = listFilesResponse.getFileNamesList();
			if (fileList != null) {
				for (String file : fileList) {
					System.out.println(file);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void debug() {
		//nameNode.test();
	}

	private static String getFileContent(String fileName) {
		File file = new File(fileName);
		String content = "";
        ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
        if (file.exists()) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String currLine;
                while ((currLine = br.readLine()) != null) {
                	content += currLine;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return content;
	}
}
