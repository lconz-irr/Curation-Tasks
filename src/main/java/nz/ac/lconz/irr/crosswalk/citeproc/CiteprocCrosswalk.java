package nz.ac.lconz.irr.crosswalk.citeproc;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.DCValue;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.crosswalk.CrosswalkException;
import org.dspace.content.crosswalk.StreamDisseminationCrosswalk;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class CiteprocCrosswalk implements StreamDisseminationCrosswalk {
	private static final Logger log = Logger.getLogger(CiteprocCrosswalk.class);

	private final Map<String,Converter> converters;
	private final Map<String,String> fields;
	private final Map<String,String> fieldTypes;

	public CiteprocCrosswalk() {
		converters = new HashMap<>();
		Properties properties = ConfigurationManager.getProperties("citeproc");
		Pattern keyPattern = Pattern.compile("^converter\\.(\\w+)$");
		for (Object key : properties.keySet()) {
			Matcher matcher = keyPattern.matcher(key.toString());
			if (matcher.matches()) {
				String name = matcher.group(1);
				try {
					String className = properties.getProperty(key.toString());
					Converter converter = (Converter) Class.forName(className).newInstance();
					converters.put(name, converter);
				} catch (ClassNotFoundException e) {
					log.error("Can't find converter class " + name, e);
				} catch (InstantiationException e) {
					log.error("Can't instantiate converter class " + name, e);
				} catch (IllegalAccessException e) {
					log.error("Not allowed to access converter class " + name, e);
				}
			}
		}

		fields = new HashMap<>();
		fieldTypes = new HashMap<>();
		keyPattern = Pattern.compile("^field\\.([a-zA-Z\\-]+)$");
		Pattern valuePattern = Pattern.compile("^([a-zA-Z\\.,]+)(?:\\((\\w+)\\))?$");
		for (Object key : properties.keySet()) {
			Matcher keyMatcher = keyPattern.matcher(key.toString());
			if (keyMatcher.matches()) {
				String field = keyMatcher.group(1);
				String value = properties.getProperty(key.toString());
				Matcher valueMatcher = valuePattern.matcher(value);
				if (valueMatcher.matches()) {
					String dcField = valueMatcher.group(1);
					fields.put(field, dcField);
					if (valueMatcher.groupCount() > 1) {
						String fieldConverter = valueMatcher.group(2);
						if (fieldConverter != null) {
							fieldTypes.put(dcField, fieldConverter);
						}
					}
				}
			}
		}
	}

	@Override
	public boolean canDisseminate(Context context, DSpaceObject dSpaceObject) {
		return dSpaceObject != null && dSpaceObject.getType() == Constants.ITEM;
	}


	@Override
	public void disseminate(Context context, DSpaceObject dSpaceObject, OutputStream outputStream) throws CrosswalkException, IOException, SQLException, AuthorizeException {
		if (!canDisseminate(context, dSpaceObject)) {
			throw new CrosswalkException("Cannot disseminate object (null or non-item object type)");
		}
		Item item = (Item) dSpaceObject;

		ObjectMapper mapper = new ObjectMapper();
		ObjectNode rootNode = mapper.createObjectNode();
		rootNode.put("id", "ITEM-1"); // ID hard-coded to what citeproc expects
		for (String fieldKey : fields.keySet()) {
			String fieldValue = fields.get(fieldKey);
			Converter converter = null;
			if (fieldTypes.containsKey(fieldValue)) {
				converter = converters.get(fieldTypes.get(fieldValue));
				if (converter == null) {
					log.warn("No converter set up for field type " + fieldTypes.get(fieldValue) + " but field " + fieldKey + " uses this converter -- not processing field");
				}
			}
			String[] mdFields = fieldValue.split(",");
			for (String mdField : mdFields) {
				DCValue[] mdValues = item.getMetadata(mdField);
				if (mdValues != null && mdValues.length > 0 && mdValues[0] != null) {
					if (converter != null) {
						converter.insertValue(rootNode, fieldKey, item, mdValues, mapper);
					} else {
						for (DCValue mdValue : mdValues) {
							String value = mdValue.value;
							if (value != null && !"".equals(value)) {
								rootNode.put(fieldKey, value);
							}
						}
					}
					break; // do not look at further fields
				}
			}
		}

		mapper.writeValue(outputStream, rootNode);
		outputStream.flush();
	}

	@Override
	public String getMIMEType() {
		return "application/json";
	}
}
