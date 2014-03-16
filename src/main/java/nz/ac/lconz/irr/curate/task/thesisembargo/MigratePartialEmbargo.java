package nz.ac.lconz.irr.curate.task.thesisembargo;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.AuthorizeManager;
import org.dspace.content.*;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Curation task to migrate DSpace 1.8 partial embargoes (LCoNZ customisation) to
 * DSpace 3/4 resource policies on the bundles and bitstreams.
 *
 * Task options:
 * - field.type: Metadata field that contains the embargo type information ("Partial")
 * - field.date: Metadata field that contains the embargo expiry date
 * - dryrun: If true, don't actually make any changes. Default: false.
 * - bundles: Comma-separated list of bundle names to operate on. Default: ORIGINAL, TEXT, THUMBNAIL.
 *
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
@Distributive
public class MigratePartialEmbargo extends AbstractCurationTask {
	private int itemsMigrated;
	private int errorItems;
	private String timestamp;

	private boolean dryRun;

	private String typeSchema;
	private String typeElement;
	private String typeQualifier;

	private String dateSchema;
	private String dateElement;
	private String dateQualifier;

	private static final Logger log = Logger.getLogger(MigratePartialEmbargo.class);
	private List<String> bundles;

	@Override
	public int perform(DSpaceObject dSpaceObject) throws IOException {
		if (typeSchema == null || typeElement == null) {
			String message = "Type field not configured";
			report(message);
			setResult(message);
			log.warn(message);
			return Curator.CURATE_ERROR;
		}
		itemsMigrated = 0;
		errorItems = 0;
		timestamp = DCDate.getCurrent().displayLocalDate(true, Locale.getDefault());

		distribute(dSpaceObject);

		String message = String.format("Migrated %d items; encountered problems migrating %d items", itemsMigrated, errorItems);
		report(message);
		setResult(message);
		if (itemsMigrated > 0) {
			return Curator.CURATE_SUCCESS;
		} else {
			return Curator.CURATE_SKIP;
		}
	}

	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);

		String typeField = taskProperty("field.type");
		if (typeField == null || "".equals(typeField)) {
			log.warn("No embargo type field set up");
			return;
		}
		String[] typeFieldParts = typeField.split("\\.");
		if (typeFieldParts.length < 2) {
			log.warn("Invalid value for embargo type field, must be schema.element or schema.element.qualifier");
			return;
		}
		typeSchema = typeFieldParts[0];
		typeElement = typeFieldParts[1];
		if (typeFieldParts.length > 2) {
			typeQualifier = typeFieldParts[2];
		}

		String dateField = taskProperty("field.date");
		if (dateField == null || "".equals(dateField)) {
			log.warn("No embargo date field set up");
			return;
		}
		String[] dateFieldParts = dateField.split("\\.");
		if (dateFieldParts.length < 2) {
			log.warn("Invalid value for embargo date field, must be schema.element or schema.element.qualifier");
			return;
		}
		dateSchema = dateFieldParts[0];
		dateElement = dateFieldParts[1];
		if (dateFieldParts.length > 2) {
			dateQualifier = dateFieldParts[2];
		}

		dryRun = taskBooleanProperty("dryrun", false);

		String bundlesProperty = taskProperty("bundles");
		String[] bundlesList;
		if (bundlesProperty != null && !"".equals(bundlesProperty)) {
			bundlesList = bundlesProperty.split(",\\s*");
		} else {
			bundlesList = new String[] { Constants.CONTENT_BUNDLE_NAME, "TEXT", "THUMBNAIL" };
		}
		bundles = Arrays.asList(bundlesList);
	}

	@Override
	protected void performItem(Item item) throws SQLException, IOException {
		if (!isPartialEmbargo(item)) {
			return; // no partial embargo -> nothing to do
		}
		DCDate embargoDate = getEmbargoDate(item);
		if (embargoDate == null || embargoDate.toDate() == null) {
			return; // no embargo date -> nothing to do
		}
		Context context = null;
		try {
			context = new Context();
			context.turnOffAuthorisationSystem();
			Bundle[] bundles = item.getBundles();
			for (Bundle bundle : bundles) {
				if (!dryRun) {
					processBundle(item, embargoDate, context, bundle);
				}
			}

			if (!dryRun) {
				item.addMetadata("dc", "description", "provenance", "en_NZ", timestamp + ": Migrated partial embargo to resource policies.");
				item.clearMetadata(typeSchema, typeElement, typeQualifier, Item.ANY);
				item.clearMetadata(dateSchema, dateElement, dateQualifier, Item.ANY);
			}
			item.update();
			context.complete();
			report("Processed item id=" + item.getID());
			itemsMigrated++;
			context = null;
		} catch (AuthorizeException e) {
			String message = "Problem setting policies for item id=" + item.getID();
			log.error(message, e);
			report(message);
			errorItems++;
		} finally {
			if (context != null && context.isValid()) {
				context.abort();
			}
		}
	}

	private void processBundle(Item item, DCDate embargoDate, Context context, Bundle bundle) throws SQLException, AuthorizeException {
		if (bundles.contains(bundle.getName())) {
			AuthorizeManager.generateAutomaticPolicies(context, embargoDate.toDate(), "Partial embargo", bundle, item.getOwningCollection());
			Bitstream[] bitstreams = bundle.getBitstreams();
			for (Bitstream bitstream : bitstreams) {
				AuthorizeManager.generateAutomaticPolicies(context, embargoDate.toDate(), "Partial embargo", bitstream, item.getOwningCollection());
			}
		}
	}

	private DCDate getEmbargoDate(Item item) {
		DCValue[] embargoDateMD = item.getMetadata(dateSchema, dateElement, dateQualifier, Item.ANY);
		if (embargoDateMD == null || embargoDateMD.length < 1 || embargoDateMD[0].value == null || "".equals(embargoDateMD[0].value)) {
			return null;
		}
		return new DCDate(embargoDateMD[0].value);
	}

	private boolean isPartialEmbargo(Item item) {
		DCValue[] embargoTypeMD = item.getMetadata(typeSchema, typeElement, typeQualifier, Item.ANY);
		if (embargoTypeMD == null || embargoTypeMD.length < 1 || embargoTypeMD[0].value == null) {
			return true;
		}
		return "Partial".equals(embargoTypeMD[0].value);
	}
}
