import java.io.*;
import java.util.*;
import NameNode.*;

public class Client {
	private static NameNode nameNode = new NameNode();
	
	public static void main(String args[]) {
		String fileName = "";
		
		if (args[0].equals("get")) {
			fileName = args[1];
		} else if (args[0].equals("put")) {
			fileName = args[1];
		} else if (args[0].equals("list")) {
			
		}
		System.out.println(fileName);
		
	}
}
