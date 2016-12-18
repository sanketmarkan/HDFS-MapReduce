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
import org.w3c.dom.NodeList;

import Utils.Constants;
// Helper class to read/write FsImage
public class FsImageHelper {
	private static DocumentBuilderFactory docBuildFactory;
	private static DocumentBuilder docBuilder;
	private static Document doc;
	private static Element root;
	private static FsImageHelper fsImage = null;
	private FsImageHelper() {
		init();
	}

	public static FsImageHelper getInstance() {
		if (fsImage == null) {
			fsImage = new FsImageHelper();
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
			} else {
				doc = docBuilder.newDocument();
				root = doc.createElementNS("http://github.com/shivtej1505/HDFS-MapReduce", "files");
		        doc.appendChild(root);
			}
		} catch (Exception e) {
	    	e.printStackTrace();
	    }
	}

	public static void addFileEntry(String fileName, int fileId, ArrayList<Integer> blockList) {
		Element fileEntry = doc.createElement("file");
		fileEntry.setAttribute("name", fileName);
		fileEntry.setAttribute("id", String.valueOf(fileId));
		Element blocks = doc.createElement("blocks");
		if (blockList != null) {
			for (Integer block : blockList) {
				Element ele= doc.createElement("block");
				ele.appendChild(doc.createTextNode(String.valueOf(block)));
				blocks.appendChild(ele);
			}
		}
		fileEntry.appendChild(blocks);
		root.appendChild(fileEntry);
		updateFsImage();
    }

    public static void addFileBlock(int fileId, int blockId) {
    	NodeList files = root.getElementsByTagName("file");
    	for(int i=0; i<files.getLength(); i++) {
    		Node file = files.item(i);
    		if (file.getNodeType() == Node.ELEMENT_NODE) {
    			Integer id = Integer.valueOf(((Element) file).getAttribute("id"));
    			if (id == fileId) {
    				NodeList list = ((Element) file).getElementsByTagName("blocks");
    				Element blocks = (Element) list.item(0);
					Element ele = doc.createElement("block");
					ele.appendChild(doc.createTextNode(String.valueOf(blockId)));
					blocks.appendChild(ele);
    			}
    		}
    	}
    	//printTree();
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

    public static HashMap<String, Integer> getFileEntries() {
    	HashMap<String, Integer> fileList = new HashMap<>();
    	NodeList files = root.getElementsByTagName("file");
    	for(int i=0; i<files.getLength(); i++) {
    		Node file = files.item(i);
    		if (file.getNodeType() == Node.ELEMENT_NODE) {
    			String fileName = ((Element) file).getAttribute("name");
    			Integer fileId = Integer.valueOf(((Element) file).getAttribute("id"));
    			fileList.put(fileName, fileId);
    			//System.out.println("name: " + fileName);
    			//System.out.println("id: " + fileId);
    		}
    	}
    	return fileList;
    }

    public static ArrayList<Integer> getFileBlocks(int fileId) {
    	ArrayList<Integer> blockList = new ArrayList<Integer>();
    	NodeList files = root.getElementsByTagName("file");
    	for(int i=0; i<files.getLength(); i++) {
    		Node file = files.item(i);
    		if (file.getNodeType() == Node.ELEMENT_NODE) {
    			Integer id = Integer.valueOf(((Element) file).getAttribute("id"));
    			if (id == fileId) {
    				Node list = ((Element) file).getElementsByTagName("blocks").item(0);
    				NodeList blocks = ((Element) list).getElementsByTagName("block");
    				for(int j=0; j<blocks.getLength(); j++) {
    					Element block = (Element) blocks.item(j);
    					String blockId = block.getTextContent();
    					blockList.add(Integer.valueOf(blockId));
    				}
    				break;
    			}
    		}
    	}
    	return blockList;
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