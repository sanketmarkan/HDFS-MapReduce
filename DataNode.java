import java.rmi.RemoteException;

import java.rmi.server.UnicastRemoteObject;

import IDataNode.*;
import Utils.*;
import java.io.*;
import java.util.*;
import java.net.*;
import INameNode.*;
import Protobuf.HDFS.*;
import com.google.protobuf.ByteString;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

public class DataNode implements IDataNode {

	private static int myId;
    private static ArrayList<Integer> blockList;
    public static void main(String args[]){
        myId = Integer.parseInt(args[0]);
        blockList = new ArrayList<Integer>();
		init();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try{
                        sendHeartBeat();
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // nope
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try{
                        sendBlockReport();
                        try {
                            Thread.sleep(1000);
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
            DataNode obj = new DataNode();
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("datanode", stub);
            System.out.println("DataNode " + myId +" ready");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	@Override
	public byte[] readBlock(byte[] inp) throws RemoteException {
		ReadBlockRequest request = (ReadBlockRequest) Utils.deserialize(inp);
		int block = request.getBlockNumber();
		String fileName = "block-"+block; 
		File file = new File(fileName);
        ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
        if (file.exists()) {
        	response.setStatus(1);
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String currLine;
                while ((currLine = br.readLine()) != null) {
                	response.addData(ByteString.copyFromUtf8(currLine));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
        	response.setStatus(0);
        }
        return Utils.serialize(response.build());
    }

	@Override
	public byte[] writeBlock(byte[] inp) throws RemoteException {
		WriteBlockRequest request = (WriteBlockRequest) Utils.deserialize(inp);
		int block = request.getBlockNumber();
		String data = request.getData(0).toStringUtf8();
		
		//System.out.println(data);

		String fileName = "block-"+block;
		File file = new File(fileName);
		// data = request.getData();
		WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
		if (file.exists()) {
			response.setStatus(0);
        }
		else {
            blockList.add(block);
			response.setStatus(1);
			try {
                FileWriter writer = new FileWriter(file, true);
                writer.write(data);
                writer.flush();
                writer.close();
                System.out.println("Wrote to file");
            } catch (Exception e) {
                e.printStackTrace();
                response.setStatus(0);
            }

		}
		return Utils.serialize(response.build());
	}

    public static byte[] sendBlockReport() throws RemoteException {
        BlockReportRequest.Builder request = BlockReportRequest.newBuilder();
        request.setId(myId);
        for(int block:blockList)
            request.addBlockNumbers(block);
        try{
            Registry registry = LocateRegistry.getRegistry(Constants.nnIp,Constants.nnPort);
            INameNode stub = (INameNode) registry.lookup("namenode");
            stub.blockReport(Utils.serialize(request.build()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

	public static void sendHeartBeat() throws RemoteException{
		try{
			// Registry registry = LocateRegistry.getRegistry(nnIP, nnPort);
            Registry registry = LocateRegistry.getRegistry(Constants.nnIp,Constants.nnPort);
	        INameNode stub = (INameNode) registry.lookup("namenode");
            HeartBeatRequest.Builder request = HeartBeatRequest.newBuilder();
	        request.setId(myId);
            String networkInterfaceName = "wlp6s0";
            Inet4Address inetAddress = null;
            try {
                Enumeration<InetAddress> enumeration = NetworkInterface.getByName(networkInterfaceName).getInetAddresses();
                while (enumeration.hasMoreElements()) {
                    InetAddress tempInetAddress = enumeration.nextElement();
                    if (tempInetAddress instanceof Inet4Address) {
                        inetAddress = (Inet4Address) tempInetAddress;
                    }
                }
            } catch (SocketException e) {
                e.printStackTrace();
            }
            if (inetAddress == null) {
                System.err.println("Error Obtaining Network Information");
                System.exit(-1);
            }
            DataNodeLocation.Builder dNbuilder = DataNodeLocation.newBuilder();
            dNbuilder.setIp(inetAddress.getHostAddress());
            dNbuilder.setPort(Registry.REGISTRY_PORT);
            request.setLocation(dNbuilder.build());
            stub.heartBeat(Utils.serialize(request.build()));
	    } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
