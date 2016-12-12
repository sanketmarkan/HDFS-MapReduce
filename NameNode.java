import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import java.util.*;
import java.util.concurrent.locks.*;
import java.io.*;
import java.text.*;

import Protobuf.HDFS.*;
import Utils.*;
import INameNode.*;

public class NameNode implements INameNode {
	private static HashMap<String, Integer> fileToInt = new HashMap<>();
	private static HashMap<Integer, ArrayList<Integer>> blockList = new HashMap<>();
	private static HashMap<String, ArrayList<String>> filesDir = new HashMap<>();
	private static HashMap<Integer, ArrayList<DataNodeLocation>> blockLocation = new HashMap<>();
	private static HashMap<Integer, Integer> handleList = new HashMap<>();
	private static HashMap<Integer, DataNodeLocation> livingDataNodes = new HashMap<>();
	private static HashMap<Integer, String> lastBeatNode = new HashMap<>();
	private static int fileCounter = 1;
	private static int blockCounter = 1;
	private static int handleCounter = 1;
	private static Lock lock = new ReentrantLock();
	private static final int STATUS_OK = 1;
	private static final int STATUS_NOT_OK = 0;

	public static void main(String args[]){
		init();
		new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try{
						DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
						Calendar cal = Calendar.getInstance();
                    	for(Integer dataNode : livingDataNodes.keySet()) {
                    		lock.lock();
                    		String last_beat = lastBeatNode.get(dataNode);
							lock.unlock();
							String time_now = dateFormat.format(cal.getTime());
							SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");

						    Date d1 = null;
						    Date d2 = null;
						    try {
						        d1 = format.parse(last_beat);
						        d2 = format.parse(time_now);
						    } catch (ParseException e) {
						        e.printStackTrace();
						    }

						    // Get msec from each, and subtract.
						    long diff = d2.getTime() - d1.getTime();
						    long diffSeconds = diff / 1000 % 60;
						    if(diffSeconds > 10) {
						    	lock.lock();
						    	livingDataNodes.remove(dataNode);
						    	lock.unlock();
						    }
                    	}
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            // nope
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
	}

    public static void init() {
    	try {
    		// grabbing offline data
    		readFsImage();
    		// binding to registry
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
				lock.lock();
				int fileId = fileCounter;
				int handle = handleCounter;
				handleCounter++;
				fileCounter++;
				handleList.put(handle, fileId);
				lock.unlock();
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
			lock.lock();
			handleList.remove(handle);
			lock.unlock();
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
		lock.lock();
		int blockId = blockCounter;
		System.out.println(fileId+" "+handle);
		blockCounter++;
		lock.unlock();
		// insert in blockList
		ArrayList<Integer> fileBlockList = blockList.get(fileId);
		if (fileBlockList == null)
			fileBlockList = new ArrayList<Integer>();
		fileBlockList.add(blockId);
		lock.lock();
		blockList.put(fileId, fileBlockList); 
		lock.unlock();
		BlockLocations.Builder blockLocations = BlockLocations.newBuilder();
		blockLocations.setBlockNumber(blockId);

		AssignBlockResponse.Builder assignBlockResponse = AssignBlockResponse.newBuilder();
		
		System.out.println(livingDataNodes.size());
		if(livingDataNodes.size() == 0) {
			assignBlockResponse.setStatus(STATUS_NOT_OK);
			return Utils.serialize(assignBlockResponse.build());
		}
		for (int num = 0 ; num<Math.min(3, (int)livingDataNodes.size());num++){
			Random rn = new Random();
			int index = rn.nextInt() % (livingDataNodes.size());
			int id = (int)livingDataNodes.keySet().toArray()[index];
			DataNodeLocation location = livingDataNodes.get(id);
			ArrayList<DataNodeLocation> fileBlockLocations = blockLocation.get(blockId);
			if (fileBlockLocations == null)
				fileBlockLocations = new ArrayList<DataNodeLocation>();
			fileBlockLocations.add(location);
			blockLocation.put(blockId, fileBlockLocations);
			blockLocations.addLocations(location);
		}
		assignBlockResponse.setStatus(STATUS_OK);
		assignBlockResponse.setNewBlock(blockLocations.build());
		return Utils.serialize(assignBlockResponse.build());
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		ListFilesRequest directory = (ListFilesRequest) Utils.deserialize(inp);
		// // test();
		// if(directory.hasDirName()){
		// 	ArrayList<String> list = filesDir.get(directory.getDirName());
		// 	return ListFilesResponse.newBuilder().setStatus(1).addAllFileNames(list).build().toByteArray();
		// }
		ListFilesResponse.Builder response = ListFilesResponse.newBuilder();
		response.setStatus(STATUS_OK);
		for(String file : fileToInt.keySet()){
			response.addFileNames(file);
		}
		return Utils.serialize(response.build());
	}

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		BlockReportRequest request = (BlockReportRequest) Utils.deserialize(inp);
		int id = request.getId();
		DataNodeLocation requestLocation = request.getLocation();
		List<Integer> blockNumbers = request.getBlockNumbersList();
		for (Integer block : blockNumbers){
			boolean found = false;
			for(DataNodeLocation location : blockLocation.get(block)){
				if(location.getIp() == requestLocation.getIp()){
					found = true;
					break;
				}
			}
			if(!found) {
				ArrayList<DataNodeLocation> blockLocations = blockLocation.get(id);
				if (blockLocations == null)
					blockLocations = new ArrayList<DataNodeLocation>();
				blockLocations.add(requestLocation);
				blockLocation.put(id, blockLocations);
			}
		}
		BlockReportResponse.Builder response = BlockReportResponse.newBuilder();
		response.addStatus(STATUS_OK);
		return Utils.serialize(response.build());
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		// REMOVE BLOCK LOCATIONS ON BASIS OF BLOCKREPORT
		// CORRESPONDING TO PREVIOUS STEP ADD NEW BLOCK LOCATIONS FOR A BLOCK

		HeartBeatRequest request = (HeartBeatRequest) Utils.deserialize(inp);
		int id  = request.getId();
		if (livingDataNodes.get(id) == null)
			livingDataNodes.put(id,request.getLocation());
		// System.out.println(livingDataNodes.get(id).getIp());
		DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		lastBeatNode.put(id,dateFormat.format(cal.getTime()));
		HeartBeatResponse.Builder response = HeartBeatResponse.newBuilder();
		response.setStatus(STATUS_OK);
		return Utils.serialize(response.build());
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

	// setup offline storage
	public static void readFsImage() {
		/*File offlineStore = new File(Constants.OFFLINE_DIR);
		if (!offlineStore.exists()) {
			System.out.println("Creating offline store");
			offlineStore.mkdir();
		}

		File fileList = new File(Constants.OFFLINE_DIR + "/" + Constants.FILE_LIST);
		if (!fileList.exists()) {
			fileList.createNewFile();
			System.out.println("Creating files list");
		}
		// load file list from the store
		printFileContent(fileList);

		File blocksList = new File(Constants.OFFLINE_DIR + "/" + Constants.BLOCKS_LIST);
		if (!blocksList.exists()) {
			blocksList.createNewFile();
			System.out.println("Creating blocks list");
		}
		// load block list from the store
		printFileContent(blocksList);*/
	}

	public static void printFileContent(File file) {
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
        System.out.println(content);
	}
}
