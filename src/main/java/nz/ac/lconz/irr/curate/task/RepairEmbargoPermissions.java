package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.AuthorizeManager;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.content.*;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;
import org.dspace.eperson.Group;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * DSpace curation task to repair the permissions of embargoed items.
 * Note that this uses the LCoNZ definition of embargo, which is a full embargo.
 *
 * @author Andrea Schweer schweer@waikato.ac.nz
 */
@Distributive
public class RepairEmbargoPermissions extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(RepairEmbargoPermissions.class);

	private static final List<String> FULL_EMBARGO_IGNORED_BUNDLES = Collections.emptyList();

	private int numSkippedItems;
	private int numOkEmbargoedItems;
	private int numFixedEmbargoedItems;
	private int readGroupId;
	private int adminGroupId;
	private String schema;
	private String element;
	private String qualifier;

	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);
		readGroupId = ConfigurationManager.getIntProperty("lconz-extras", "thesisembargo.read.groupid", 1);
		adminGroupId = ConfigurationManager.getIntProperty("lconz-extras", "thesisembargo.admin.groupid", 1);
		String dateField = ConfigurationManager.getProperty("lconz-extras", "thesisembargo.field");
		if (dateField == null || "".equals(dateField)) {
			log.warn("No embargo field set up");
			return;
		}
		String[] fieldParts = dateField.split("\\.");
		if (fieldParts.length < 2) {
			log.warn("Invalid value for embargo date field, must be schema.element or schema.element.qualifier");
			return;
		}
		schema = fieldParts[0];
		element = fieldParts[1];
		if (fieldParts.length > 2) {
			qualifier = fieldParts[2];
		}
	}

	/**
	 * Repair embargo permissions of an item.
	 *
	 * @param dso the object on which to perform this task. Must be of type Item.
	 * @return Curator.CURATE_SKIP if the dspace object isn't an item; Curator.CURATE_FAIL if the item is not embargoed
	 *         or if the permissions were correct already; Curator.CURATE_SUCCESS if any permissions were corrected;
	 *         Curator.CURATE_ERROR if there are any errors performing the task.
	 * @throws IOException if there are any exceptions (can wrap an SQLException or an AuthorizeException).
	 */
	@Override
	public int perform(DSpaceObject dso) throws IOException {
		numSkippedItems = 0;
        numOkEmbargoedItems = 0;
		numFixedEmbargoedItems = 0;
        distribute(dso);
        formatResults();
        if (numFixedEmbargoedItems > 0) {
	        return Curator.CURATE_SUCCESS;
        } else if (numOkEmbargoedItems > 0) {
	        return Curator.CURATE_FAIL;
        } else {
	        return Curator.CURATE_SKIP;
        }
	}

	private void formatResults() {
		StringBuilder builder = new StringBuilder();
		builder.append(numFixedEmbargoedItems);
		builder.append(" embargoed items with incorrect permissions (fixed)");
		builder.append("\n");
		builder.append(numOkEmbargoedItems);
		builder.append(" embargoed items with correct permissions (skipped)");
		builder.append("\n");
		builder.append(numSkippedItems);
		builder.append(" non-embargoed items (skipped)");
		builder.append("\n");

		String result = builder.toString();
		report(result);
		setResult(result);
	}


	@Override
    protected void performItem(Item item) throws SQLException, IOException
    {
	    if (getLiftDate(item) == null) {
		    numSkippedItems++;
		    return;
	    }

		Context context = null;
		try {
			context = new Context();
			context.ignoreAuthorization();

			// the item is embargoed
			boolean permissionsOk = checkPermissions(context, item);

			if (!permissionsOk) {
				fixPermissions(context, item);
				context.complete();
				context = null;
				numFixedEmbargoedItems++;
			} else {
				context.abort();
				context = null;
				numOkEmbargoedItems++;
			}
		} catch (SQLException e) {
			throw new IOException(e);
		} catch (AuthorizeException e) {
			throw new IOException(e);
		} finally {
			if (context != null && context.isValid()) {
				context.abort();
			}
		}
	}

	private void fixPermissions(Context context, Item item) throws SQLException, AuthorizeException {
		makeItemAuthorisedReadOnly(context, item);
		makeBundlesBitstreamsAuthorisedReadOnly(context, item, FULL_EMBARGO_IGNORED_BUNDLES);
		if (item.isDiscoverable()) {
			item.setDiscoverable(false);
			item.update();
		}
	}

	public boolean checkPermissions(Context context, Item item) throws SQLException {
		boolean allOk = true;
		if (item.isDiscoverable()) {
			allOk = false;
			report("Item id=" + item.getID() + " is discoverable");
		}
		if (!checkItemRead(context, item)) {
			allOk = false;
			report("Item id=" + item.getID() + " is readable but shouldn't be");
		}
		if (!checkBundleBitstreamsRead(context, item, FULL_EMBARGO_IGNORED_BUNDLES)) {
			allOk = false;
			report("Item id=" + item.getID() + " has readable files/bitstreams but shouldn't have");
		}
		return allOk;
	}

	public DCDate getLiftDate(Item item) {
		DCValue[] md = item.getMetadata(schema, element, qualifier, Item.ANY);
		if (md == null || md.length < 1 || md[0].value == null || "".equals(md[0].value)) {
			return null;
		}
		return new DCDate(md[0].value);
	}

	private boolean checkItemRead(Context context, Item item) throws SQLException {
		// check for ANY read policies and report them (unless they are in the authorised group):
		for (ResourcePolicy rp : AuthorizeManager.getPoliciesActionFilter(context, item, Constants.READ))
		{
			// don't warn for authorised users
			if (rp.getGroupID() != -1 && (rp.getGroupID() == readGroupId || rp.getGroupID() == adminGroupId))
			{
				continue;
			}
			// do warn for everyone else
			report("CHECK WARNING: Item " + item.getHandle() + " allows READ by "
					       + ((rp.getEPersonID() < 0) ? "Group " + rp.getGroup().getName()
							          : "EPerson " + rp.getEPerson().getFullName()));
			return false;
		}
		// verify that embargo read group can actually read
		Group authorisedGroup = Group.find(context, readGroupId);
		if (authorisedGroup == null) {
			authorisedGroup = Group.find(context, 1);
		}
		Group[] readGroups = AuthorizeManager.getAuthorizedGroups(context, item, Constants.READ);
		for (Group group : readGroups) {
			if (group.getID() == readGroupId || authorisedGroup.isMember(group)) {
				return true; // if we made it here then all is good if read group can read
			}
		}
		return false;
	}

	private boolean checkBundleBitstreamsRead(Context context, Item item, List<String> ignoredBundles) throws SQLException {
		Group authorisedGroup = Group.find(context, readGroupId);
		if (authorisedGroup == null) {
			authorisedGroup = Group.find(context, 1);
		}

		for (Bundle bundle : item.getBundles())
		{
			String bundleName = bundle.getName();
			if (ignoredBundles.contains(bundleName))
			{
				continue; // don't check these bundles
			}
			// check for ANY read policies and report them:
			for (ResourcePolicy rp : AuthorizeManager.getPoliciesActionFilter(context, bundle, Constants.READ))
			{
				// don't warn for authorised users
				if (rp.getGroupID() != -1 && (rp.getGroupID() == readGroupId || rp.getGroupID() == adminGroupId))
				{
					continue;
				}
				report("CHECK WARNING: Item " + item.getHandle() + ", Bundle " + bundleName + " allows READ by "
						       + ((rp.getEPersonID() < 0) ? "Group " + rp.getGroup().getName()
								          : "EPerson " + rp.getEPerson().getFullName()));
				return false;
			}
			// verify that read group can read
			boolean authorisedCanReadBundle = false;
			Group[] bundleReadGroups = AuthorizeManager.getAuthorizedGroups(context, bundle, Constants.READ);
			for (Group group : bundleReadGroups) {
				if (group.getID() == readGroupId || authorisedGroup.isMember(group)) {
					authorisedCanReadBundle = true;
					break;
				}
			}
			if (!authorisedCanReadBundle) {
				return false;
			}

			for (Bitstream bs : bundle.getBitstreams())
			{
				for (ResourcePolicy rp : AuthorizeManager.getPoliciesActionFilter(context, bs, Constants.READ))
				{
					// don't warn for authorised users
					if (rp.getGroupID() != -1 && (rp.getGroupID() == readGroupId || rp.getGroupID() == adminGroupId))
					{
						continue;
					}
					report("CHECK WARNING: Item " + item.getHandle() + ", Bitstream " + bs.getName() + " (in Bundle " + bundleName + ") allows READ by "
							         + ((rp.getEPersonID() < 0) ? "Group " + rp.getGroup().getName()
									            : "EPerson " + rp.getEPerson().getFullName()));
					return false;
				}
				boolean authorisedCanReadBitstream = false;
				Group[] bitstreamReadGroups = AuthorizeManager.getAuthorizedGroups(context, bs, Constants.READ);
				for (Group group : bitstreamReadGroups) {
					if (group.getID() == readGroupId || authorisedGroup.isMember(group)) {
						authorisedCanReadBitstream = true;
						break;
					}
				}
				if (!authorisedCanReadBitstream) {
					return false;
				}
			}
		}
		// if we made it here then all is good
		return true;
	}

	private void makeBundlesBitstreamsAuthorisedReadOnly(Context context, Item item, List<String> ignoreBundles) throws SQLException, AuthorizeException {
		// remove read access from all bundles and their bitstreams
		for (Bundle bundle : item.getBundles()) {
			if (ignoreBundles.contains(bundle.getName())) {
				continue;
			}
			// remove all non-custom access
			AuthorizeManager.removeAllPoliciesByDSOAndTypeNotEqualsTo(context, bundle, ResourcePolicy.TYPE_CUSTOM);

			// add authorised group access back in
			if (readGroupId >= 0 && !AuthorizeManager.isAnIdenticalPolicyAlreadyInPlace(context, bundle, readGroupId, Constants.READ, -1)) {
				ResourcePolicy policy = AuthorizeManager.createOrModifyPolicy(null, context, "Thesis Embargo", readGroupId, null, null, Constants.READ, "Set by repair permissions curation task", bundle);
				if (policy != null) {
					policy.update();
				}
			}

			for (Bitstream bitstream : bundle.getBitstreams())
			{
				// remove all non-custom access
				AuthorizeManager.removeAllPoliciesByDSOAndTypeNotEqualsTo(context, bitstream, ResourcePolicy.TYPE_CUSTOM);

				// add authorised group access back in
				if (readGroupId >= 0 && !AuthorizeManager.isAnIdenticalPolicyAlreadyInPlace(context, bitstream, readGroupId, Constants.READ, -1)) {
					ResourcePolicy policy = AuthorizeManager.createOrModifyPolicy(null, context, "Embargo permission", readGroupId, null, null, Constants.READ, "Set by repair permissions curation task", bitstream);
					if (policy != null) {
						policy.update();
					}
				}
			}
		}
	}

	private void makeItemAuthorisedReadOnly(Context context, Item item) throws SQLException, AuthorizeException {
		AuthorizeManager.removeAllPoliciesByDSOAndTypeNotEqualsTo(context, item, ResourcePolicy.TYPE_CUSTOM);

		// but allow authorised groups to read item
		if (readGroupId >= 0)
		{
			if (!AuthorizeManager.isAnIdenticalPolicyAlreadyInPlace(context, item, readGroupId, Constants.READ, -1)) {
				ResourcePolicy policy = AuthorizeManager.createOrModifyPolicy(null, context, "Embargo permission", readGroupId, null, null, Constants.READ, "Set by repair permissions curation task", item);
				if (policy != null) {
					policy.update();
				}
			}
		}
	}
}
