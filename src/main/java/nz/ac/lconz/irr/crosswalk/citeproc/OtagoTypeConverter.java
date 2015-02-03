package nz.ac.lconz.irr.crosswalk.citeproc;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.dspace.content.Item;
import org.dspace.content.Metadatum;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class OtagoTypeConverter implements Converter {
	@Override
	public void insertValue(ObjectNode rootNode, String field, Item item, Metadatum[] mdValues, ObjectMapper mapper) {
		for (Metadatum mdValue : mdValues) {
			if (mdValue == null || mdValue.value == null) {
				continue;
			}
			String value = mdValue.value.toLowerCase();
			if (value.contains("report") || value.equals("working paper") || value.equals("discussion paper")) {
				rootNode.put(field, "report");
				Metadatum[] seriesValues = item.getMetadataByMetadataString("dc.relation.ispartofseries");
				if (seriesValues != null && seriesValues.length > 0 && seriesValues[0] != null) {
					String series = seriesValues[0].value;
					if (series != null && !"".equals(series)) {
						rootNode.put("genre", series);
					}
				} else {
					rootNode.put("genre", mdValue.value);
				}
				Metadatum[] uris = item.getMetadataByMetadataString("dc.identifier.uri");
				if (uris != null && uris.length > 0 && uris[0] != null) {
					String uriString = uris[0].value;
					if (uriString != null && !"".equals(uriString)) {
						rootNode.put("URL", uriString);
					}
				}
			} else if (value.equals("thesis") || value.equals("dissertation")) {
				rootNode.put(field, "thesis");

				StringBuilder genreBuilder = new StringBuilder(mdValue.value);
				Metadatum[] degreeNames = item.getMetadataByMetadataString("thesis.degree.name");
				if (degreeNames != null && degreeNames.length > 0 && degreeNames[0] != null) {
					String degreeName = degreeNames[0].value;
					if (degreeName != null && !"".equals(degreeName)) {
						genreBuilder.append(", ");
						genreBuilder.append(degreeName);
					}
				}
				rootNode.put("genre", genreBuilder.toString());
			} else if (value.equals("journal article")) {
				rootNode.put(field, "article-journal");
			} else if (value.contains("conference") && value.contains("paper")) {
				rootNode.put(field, "paper-conference");
			} else if (value.equals("book")) {
				rootNode.put(field, "book");
				Metadatum[] uris = item.getMetadataByMetadataString("dc.identifier.uri");
				if (uris != null && uris.length > 0 && uris[0] != null) {
					String uriString = uris[0].value;
					if (uriString != null && !"".equals(uriString)) {
						rootNode.put("URL", uriString);
					}
				}
			} else if (value.contains("chapter")) {
				rootNode.put(field, "chapter");
			} else if (value.contains("musical score")) {
				rootNode.put(field, "musical_score");
			} else if (value.equals("website")) {
				rootNode.put(field, "webpage");
			} else {
				rootNode.put(field, "article");
			}
		}
	}

}
