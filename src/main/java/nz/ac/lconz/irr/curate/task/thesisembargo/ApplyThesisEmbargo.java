package nz.ac.lconz.irr.curate.task.thesisembargo;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.AuthorizeManager;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.content.*;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
@Distributive
public class ApplyThesisEmbargo extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(ExpiringEmbargoesReminder.class);

	private String dateSchema;
	private String dateElement;
	private String dateQualifier;

	int processedItems;

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		// reset state
		processedItems = 0;

		// do work
		distribute(dso);

		// communicate results
		if (processedItems > 0) {
			return Curator.CURATE_SUCCESS;
		} else {
			return Curator.CURATE_SKIP;
		}
	}

	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);

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
	}

	@Override
	protected void performItem(Item item) throws SQLException, IOException {
		if (item.isArchived() || item.isWithdrawn()) {
			return;
		}

		Metadatum[] dateMetadata = item.getMetadata(dateSchema, dateElement, dateQualifier, Item.ANY);
		if (dateMetadata == null || dateMetadata.length == 0) {
			return; // item isn't embargoed
		}
		DCDate embargoDate = new DCDate(dateMetadata[0].value);
		if (embargoDate.toDate() == null) {
			return;  // item isn't embargoed
		}


		Context context = null;
		try {
			context = new Context();
			context.turnOffAuthorisationSystem();

			item.setDiscoverable(true);

			AuthorizeManager.removeAllPoliciesByDSOAndType(context, item, ResourcePolicy.TYPE_INHERITED);
			ResourcePolicy policy = AuthorizeManager.createOrModifyPolicy(null, context, "Thesis embargo", 1, null, null, Constants.READ, "Thesis embargo", item);
			if (policy != null) {
				policy.update();
			}

			Bundle[] bundles = item.getBundles();
			for (Bundle bundle : bundles) {
				AuthorizeManager.removeAllPoliciesByDSOAndType(context, bundle, ResourcePolicy.TYPE_INHERITED);
				policy = AuthorizeManager.createOrModifyPolicy(null, context, "Thesis embargo", 1, null, null, Constants.READ, "Thesis embargo", bundle);
				if (policy != null) {
					policy.update();
				}

				Bitstream[] bitstreams = bundle.getBitstreams();
				for (Bitstream bitstream : bitstreams) {
					AuthorizeManager.removeAllPoliciesByDSOAndType(context, bitstream, ResourcePolicy.TYPE_INHERITED);
					policy = AuthorizeManager.createOrModifyPolicy(null, context, "Thesis embargo", 1, null, null, Constants.READ, "Thesis embargo", bitstream);
					if (policy != null) {
						policy.update();
					}
				}
			}
			item.update();
			context.complete();
			context = null;
			processedItems++;
		} catch (AuthorizeException e) {
			log.error("Problem applying thesis embargo", e);
		} finally {
			if (context != null && context.isValid()) {
				context.abort();
			}
		}
	}
}
