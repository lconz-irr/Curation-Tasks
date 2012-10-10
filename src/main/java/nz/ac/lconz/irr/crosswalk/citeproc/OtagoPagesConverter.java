package nz.ac.lconz.irr.crosswalk.citeproc;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.dspace.content.DCValue;
import org.dspace.content.Item;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class OtagoPagesConverter implements Converter {
	@Override
	public void insertValue(ObjectNode rootNode, String field, Item item, DCValue[] mdValue, ObjectMapper mapper) {
		if (mdValue != null && mdValue.length > 0 && mdValue[0].value != null) {
			String valueString = mdValue[0].value;
			if (valueString == null || "".equals(valueString)) {
				return;
			}
			if (valueString.contains("-")) {
				rootNode.put(field, valueString);
			} else {
				StringBuilder pageBuilder = new StringBuilder(valueString);
				DCValue[] morePages = item.getMetadata("otago.bitstream.endpage");
				if (morePages != null && morePages.length > 0 && morePages[0] != null) {
					String morePage = morePages[0].value;
					if (morePage != null && !"".equals(morePage)) {
						pageBuilder.append("-");
						pageBuilder.append(morePage);
					}
				}
				rootNode.put(field, pageBuilder.toString());
			}
		}
	}
}
