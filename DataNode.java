import java.rmi.RemoteException;
import IDataNode.*;
import Utils.*;
import java.io.*;
import java.util.*;

import Protobuf.HDFS.*;

public class DataNode implements IDataNode {
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
		WriteBlockResponse response = WriteBlockResponse.newBuilder();
		if(file.exists()){
			response.setStatus(0);
		}
		else {
			response.setStatus(1);
			// WRITE TO FILE BRO
		}
		return Utils.serialize(response.build());
	}

}
