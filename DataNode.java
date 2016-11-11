import java.rmi.RemoteException;

import java.rmi.server.UnicastRemoteObject;

import IDataNode.*;
import Utils.*;
import java.io.*;
import java.util.*;
import INameNode.*;
import Protobuf.HDFS.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;

public class DataNode implements IDataNode {

	private static int myId;
	public static void main(String args[]){
        myId = Integer.parseInt(args[0]);
		init();
	}

    public static void init() {
    	try {
            NameNode obj = new NameNode();
            INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("datanode-"+myId, stub);
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
                	// response.addData(copyFrom(Utils.serialize(currLine)));
                	//
                	//
                	// FIGURE OUT A WAY TO COPY TO BYTESTRING.
                	//
                	//
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
		String fileName = "block-"+block;
		File file = new File(fileName);
		// data = request.getData();
		WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
		if(file.exists()){
			response.setStatus(0);
		}
		else {
			response.setStatus(1);
			// WRITE TO FILE BRO
		}
		return Utils.serialize(response.build());
	}

	public void sendHeartBeat() throws RemoteException{
		try{
			Registry registry = LocateRegistry.getRegistry();
	        INameNode stub = (INameNode) registry.lookup("namenode");
	        stub.heartBeat();
	    } catch (Exception e) {
            e.printStackTrace();
        }
	}

}
