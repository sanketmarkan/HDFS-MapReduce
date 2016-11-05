package dsproject;
import java.io.*;
import java.util.*;

public class Utils {
	public static byte[] serialize(Object object) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            out.flush();
            byte[] bArray = bos.toByteArray();
            return bArray;
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

}