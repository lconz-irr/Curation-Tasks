package nz.ac.lconz.irr.crosswalk.citeproc;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.dspace.content.DCValue;
import org.dspace.content.Item;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class UoWTypesConverter implements Converter {
	@Override
	public void insertValue(ObjectNode rootNode, String field, Item item, DCValue[] mdValues, ObjectMapper mapper) {
		for (DCValue mdValue : mdValues) {
			if (mdValue == null || mdValue.value == null) {
				continue;
			}
			String value = mdValue.value.toLowerCase();
			if (value.contains("report") || value.equals("working paper") || value.equals("discussion paper")) {
				processReport(rootNode, field, item, mdValue);
			} else if (value.equals("thesis") || value.equals("dissertation")) {
				rootNode.put(field, "thesis");

				String genreSourceField = "thesis.degree.name";
				StringBuilder genreBuilder = new StringBuilder(mdValue.value);
				DCValue[] degreeNames = item.getMetadata(genreSourceField);
				if (degreeNames != null && degreeNames.length > 0 && degreeNames[0] != null) {
					String degreeName = degreeNames[0].value;
					if (degreeName != null && !"".equals(degreeName)) {
						genreBuilder.append(", ");
						genreBuilder.append(degreeName);
					}
				}
				rootNode.put("genre", genreBuilder.toString());
				addUrlFromUri(rootNode, item);
			} else if (value.equals("journal article")) {
				rootNode.put(field, "article-journal");
			} else if (value.contains("conference") || value.equals("oral presentation")) {
				if (hasProceedingsInfo(item)) {
					rootNode.put(field, "chapter");
				} else {
					rootNode.put(field, "paper-conference");
				}
			} else if (value.contains("chapter")) {
				rootNode.put(field, "chapter");
			} else if (value.contains("book") || value.equals("scholarly edition") || value.equals("monograph")) {
				rootNode.put(field, "book");
			} else if (value.contains("musical score")) {
				rootNode.put(field, "musical_score");
			} else if (value.equals("website")) {
				rootNode.put(field, "webpage");
			} else {
				rootNode.put(field, "article");
				rootNode.put("genre", value); // put the actual type into the genre field
			}
		}
	}

	private boolean hasProceedingsInfo(Item item) {
		DCValue[] partOfs = item.getMetadata("dc.relation.isPartOf");
		for (DCValue partOf : partOfs) {
			if (StringUtils.isNotBlank(partOf.value)) {
				return true;
			}
		}
		return false; // haven't found anything
	}

	private void processReport(ObjectNode rootNode, String field, Item item, DCValue mdValue) {
		rootNode.put(field, "report");
		DCValue[] seriesValues = item.getMetadata("dc.relation.ispartofseries");
		if (seriesValues != null && seriesValues.length > 0 && seriesValues[0] != null) {
			String series = seriesValues[0].value;
			if (series != null && !"".equals(series)) {
				rootNode.put("genre", series);
			}
		} else {
			rootNode.put("genre", mdValue.value);
		}
	}

	private void addUrlFromUri(ObjectNode rootNode, Item item) {
		DCValue[] uris = item.getMetadata("dc.identifier.uri");
		if (uris != null && uris.length > 0 && uris[0] != null) {
			String uriString = uris[0].value;
			if (uriString != null && !"".equals(uriString)) {
				rootNode.put("URL", uriString);
			}
		}
	}
}
