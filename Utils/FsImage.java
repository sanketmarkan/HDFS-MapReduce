package Utils;
import java.io.*;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
 
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import Utils.Constants;
// Helper class to read/write FsImage
public class FsImage {
	private static DocumentBuilderFactory docBuildFactory;
	private static DocumentBuilder docBuilder;
	private static Document doc;
	private static Element root;
	private static FsImage fsImage = null;
	private FsImage() {
		init();			
	}

	public static FsImage getInstance() {
		if (fsImage == null) {
			fsImage = new FsImage();
		}
		return fsImage;
	}

	public static void init() {
		try {
			docBuildFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docBuildFactory.newDocumentBuilder();

			File inputFile = new File(Constants.FS_IMAGE_FILE);
			if (inputFile.exists()) {
				doc = docBuilder.parse(inputFile);
				root = doc.getDocumentElement();
				System.out.println("yeah");
			} else {
				doc = docBuilder.newDocument();
				root = doc.createElementNS("http://github.com/shivtej1505/HDFS-MapReduce", "files");
		        doc.appendChild(root);
			}
		} catch (Exception e) {
	    	e.printStackTrace();
	    }
	}

	public static void main(String[] args) {
		FsImage fs = FsImage.getInstance();
		ArrayList<Integer> list = new ArrayList<Integer>();
		list.add(1);
		list.add(4);
		list.add(11);
		fs.addFileEntry("asdf", 1, list);
		fs.addFileEntry("adf", 5, list);
		fs.addFileEntry("lol", 14, list); 
		printTree();
	}

	public static void addFileEntry(String fileName, int fileId, ArrayList<Integer> blockList) {
		Element fileEntry = doc.createElement("file");
		fileEntry.setAttribute("name", fileName);
		fileEntry.setAttribute("id", String.valueOf(fileId));
		Element blocks = doc.createElement("blocks");
		for (Integer block : blockList) {
			Element ele= doc.createElement("block");
			ele.appendChild(doc.createTextNode(String.valueOf(block)));
			blocks.appendChild(ele);
		}
		fileEntry.appendChild(blocks);
		root.appendChild(fileEntry);
		updateFsImage();
    }

    public static void updateFsImage() {
    	try {
	    	Transformer transformer = TransformerFactory.newInstance().newTransformer();
	        transformer.setOutputProperty(OutputKeys.INDENT, "yes"); 
	        DOMSource source = new DOMSource(doc);
	        StreamResult result = new StreamResult(new File("fsImage.xml"));
	        transformer.transform(source, result);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
    }

    public static void getFileEntries() {
    }

    public static void printTree() {
    	try {
	    	Transformer transformer = TransformerFactory.newInstance().newTransformer();
	        transformer.setOutputProperty(OutputKeys.INDENT, "yes"); 
	        DOMSource source = new DOMSource(doc);
	        StreamResult console = new StreamResult(System.out);
	        transformer.transform(source, console);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}
} 