import IMapper.*;

public class MapNode implements IMapper {
	private String key = "key"; 
	private int lastIndex = 0;
	private int count = 0;
	public String map(String inp) {
		while (lastIndex != -1) {
		    lastIndex = inp.indexOf(key, lastIndex);
		    if (lastIndex != -1) {
		        count++;
		        lastIndex += key.length();
		    }
		}
		return key + ":" + count;
	}	
}