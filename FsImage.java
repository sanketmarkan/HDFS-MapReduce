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

// Helper class to read/write FsImage
public class FsImage {
	private static DocumentBuilderFactory docBuildFactory;
	private static DocumentBuilder docBuilder;
	private static Document doc;
	private static Element root;
	private static FsImage fsImage = null;
	private FsImage() {
		try {
			docBuildFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docBuildFactory.newDocumentBuilder();
			doc = docBuilder.newDocument();
			root = doc.createElementNS("http://github.com/shivtej1505", "Files");
	        doc.appendChild(root);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	}

	public static FsImage getInstance() {
		if (fsImage == null) {
			fsImage = new FsImage();
		}
		return fsImage;
	}

	public static void main(String[] args) {
		FsImage fs = FsImage.getInstance();
		fs.addFileEntry("asdf");
		fs.addFileEntry("adf");
		fs.addFileEntry("asd");
		printTree();
	}

	public static void addFileEntry(String fileName) {
		root.appendChild(doc.createElement(fileName));
        
        /*
        mainRootElement.appendChild(getCompany(doc, "1", "Paypal", "Payment", "1000"));
        mainRootElement.appendChild(getCompany(doc, "2", "eBay", "Shopping", "2000"));
        mainRootElement.appendChild(getCompany(doc, "3", "Google", "Search", "3000"));
		*/
    }

    public static void printTree() {
    	try {
	    	Transformer transformer = TransformerFactory.newInstance().newTransformer();
	        transformer.setOutputProperty(OutputKeys.INDENT, "yes"); 
	        DOMSource source = new DOMSource(doc);
	        StreamResult console = new StreamResult(System.out);
	        transformer.transform(source, console);
	    } catch (Exception e) {

	    }
	}

    private static Node getCompany(Document doc, String id, String name, String age, String role) {
        Element company = doc.createElement("Company");
        company.setAttribute("id", id);
        company.appendChild(getCompanyElements(doc, company, "Name", name));
        company.appendChild(getCompanyElements(doc, company, "Type", age));
        company.appendChild(getCompanyElements(doc, company, "Employees", role));
        return company;
    }
 
    // utility method to create text node
    private static Node getCompanyElements(Document doc, Element element, String name, String value) {
        Element node = doc.createElement(name);
        node.appendChild(doc.createTextNode(value));
        return node;
    }
} 