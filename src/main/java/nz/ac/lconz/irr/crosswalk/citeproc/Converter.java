package nz.ac.lconz.irr.crosswalk.citeproc;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.dspace.content.Item;
import org.dspace.content.Metadatum;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public interface Converter {
	void insertValue(ObjectNode rootNode, String field, Item item, Metadatum[] mdValue, ObjectMapper mapper);
}
