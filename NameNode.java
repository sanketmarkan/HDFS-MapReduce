import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

import Protobuf.HDFS.DataNodeLocation;
import Protobuf.HDFS.ListFilesResponse;
import Protobuf.HDFS.ListFilesRequest;

import INameNode.*;

public class NameNode implements INameNode {

	private static HashMap<String, Integer> fileToInt = new HashMap<>();
	private static HashMap<Integer, ArrayList<Integer>> blockList = new HashMap<>();
	private static HashMap<String, ArrayList<String>> filesDir = new HashMap<>();
	private static HashMap<Integer, ArrayList<DataNodeLocation>> blockLocation = new HashMap<>();
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
	
	public static byte[] serialize(Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            byte[] yourBytes = bos.toByteArray();
            return yourBytes;
        } catch (Exception e) {
            return null;
        }
    }

    public static Object deserialize(byte[] response) {
        ByteArrayInputStream bis = new ByteArrayInputStream(response);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (Exception e) {
            return null;
        }
    }

	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		String fileName = new String(inp);
		return null;
	}

	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		String fileName = new String(inp);
		return null;
	}

	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		ListFilesRequest directory = (ListFilesRequest) deserialize(inp);
		if(directory.hasDirName()){
			ArrayList<String> list = filesDir.get(directory.getDirName());
			return ListFilesResponse.newBuilder().setStatus(1).addAllFileNames(list).build().toByteArray();
		}
		return ListFilesResponse.newBuilder().setStatus(1).addAllFileNames(fileToInt.keySet()).build().toByteArray();
	}

	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

}
