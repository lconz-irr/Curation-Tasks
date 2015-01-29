package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.DCValue;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.authority.ChoiceAuthorityManager;
import org.dspace.content.authority.Choices;
import org.dspace.content.authority.MetadataAuthorityManager;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Mutative;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for LCoNZ
 */
@Mutative
public class RetroactivelyAssignAuthorityValue extends AbstractCurationTask {

	private static final Logger log = Logger.getLogger(RetroactivelyAssignAuthorityValue.class);

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso.getType() != Constants.ITEM) {
			return Curator.CURATE_SKIP;
		}

		Item item = (Item) dso;
		List<String> fields = getFields();

		if (fields.isEmpty()) {
			String message = "No fields configured for retroactively assigning authority value -- doing nothing";
			report(message);
			setResult(message);
			return Curator.CURATE_SKIP;
		}

		try {
			ChoiceAuthorityManager authorityManager = ChoiceAuthorityManager.getManager();
			int owningCollection = item.getOwningCollection().getID();


			for (String field : fields) {
				processField(item, authorityManager, owningCollection, field);
			}

			item.update();

			String message = "Retroactively assigned authority values for item id=" + item.getID() + " (fields are " + fields.toString() + ")";
			report(message);
			setResult(message);
			return Curator.CURATE_SUCCESS;
		} catch (SQLException e) {
			String message = "Problem processing item id=" + item.getID() + ": " + e.getMessage();
			log.warn(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} catch (AuthorizeException e) {
			String message = "Not authorised to process item id=" + item.getID() + ": " + e.getMessage();
			log.warn(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}
	}

	private void processField(Item item, ChoiceAuthorityManager authorityManager, int owningCollection, String field) {
		log.info("Processing item id=" + item.getID() + ", field=" + field);

		String schema;
		String element;
		String qualifier = null;

		String[] fieldComponents = field.split("\\.");
		schema = fieldComponents[0];
		element = fieldComponents[1];
		if (fieldComponents.length > 2) {
			qualifier = fieldComponents[2];
		}

		DCValue[] values = item.getMetadata(schema, element, qualifier, Item.ANY);
		item.clearMetadata(schema, element, qualifier, Item.ANY);

		for (DCValue existingValue : values) {
			if (existingValue.authority != null && existingValue.confidence > Choices.CF_REJECTED) {
				// already has valid authority value -> restore previous value (effectively keep whatever was there)
				log.info("Item id=" + item.getID() + " already has an authority for its value (" + existingValue.value + "), keeping it");
				item.addMetadata(schema, element, qualifier, existingValue.language, existingValue.value, existingValue.authority, existingValue.confidence);
			} else {
				// otherwise: look up in authority for this field
				Choices bestMatch = authorityManager.getBestMatch(field.replaceAll("\\.", "_"), existingValue.value, owningCollection, existingValue.language);
				log.info("Looking up best match for field " + field + ", value " + existingValue.value);
				if (bestMatch.confidence > Choices.CF_NOTFOUND && bestMatch.values.length > 0) {
					// found a good authority value -> set it
					log.info("Found good value " + bestMatch.values[0].value + " with authority " + bestMatch.values[0].authority + ", confidence=" + Choices.getConfidenceText(bestMatch.confidence) + ", assigning authority");
					item.addMetadata(schema, element, qualifier, existingValue.language, existingValue.value, bestMatch.values[0].authority, bestMatch.confidence);
				} else {
					log.info("Did't find good value, keeping old value");
					// did not find a good authority value -> keep previous value
					item.addMetadata(schema, element, qualifier, existingValue.language, existingValue.value, null, bestMatch.confidence);
				}
			}
		}
	}

	private List<String> getFields() {
		List<String> fields = new ArrayList<String>();
		MetadataAuthorityManager manager = MetadataAuthorityManager.getManager();

		String fieldsProperty = ConfigurationManager.getProperty("authority", "fields");
		log.info("fields configuration value is " + fieldsProperty);
		String[] fieldCandidates = null;
		if (fieldsProperty != null && !"".equals(fieldsProperty.trim())) {
			fieldCandidates = fieldsProperty.trim().split(",\\s*");
		}

		if (fieldCandidates == null) {
			return fields;
		}

		for (String fieldCandidate : fieldCandidates) {
			if (manager.isAuthorityControlled(fieldCandidate.replaceAll("\\.", "_"))) {
				fields.add(fieldCandidate);
			}
		}
		return fields;
	}
}
