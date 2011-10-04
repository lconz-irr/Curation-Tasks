package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.AuthorizeManager;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;
import org.dspace.embargo.EmbargoManager;
import org.dspace.eperson.Group;

import java.io.IOException;
import java.sql.SQLException;

/**
 * DSpace curation task to repair the permissions of embargoed items.
 * Note that this uses the LCoNZ definition of embargo, which is a full embargo.
 *
 * @author Andrea Schweer schweer@waikato.ac.nz
 */
@Distributive
public class RepairEmbargoPermissions extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(RepairEmbargoPermissions.class);

	private int numSkippedItems;
	private int numOkEmbargoedItems;
	private int numFixedEmbargoedItems;

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
        return Curator.CURATE_SUCCESS;
	}

	private void formatResults() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(numFixedEmbargoedItems);
		buffer.append(" embargoed items with incorrect permissions (fixed)");
		buffer.append("\n");
		buffer.append(numOkEmbargoedItems);
		buffer.append(" embargoed items with correct permissions (skipped)");
		buffer.append("\n");
		buffer.append(numSkippedItems);
		buffer.append(" non-embargoed items (skipped)");
		buffer.append("\n");

		String result = buffer.toString();
		report(result);
		setResult(result);
	}


	@Override
    protected void performItem(Item item) throws SQLException, IOException
    {
		Context context = null;
		try {
			context = new Context();
			context.ignoreAuthorization();
			if (EmbargoManager.getEmbargoDate(context, item) == null) {
				context.abort();
				context = null;
				numSkippedItems++;
				return;
			}

			// the item is embargoed
			boolean permissionsOk = checkPermissions(context, item);

			if (!permissionsOk) {
				fixPermissions(context, item);
				context.complete();
				context = null;
				numFixedEmbargoedItems++;
			} else {
				numOkEmbargoedItems++;
			}
		} catch (SQLException e) {
			throw new IOException(e);
		} catch (AuthorizeException e) {
			throw new IOException(e);
		} finally {
			if (context != null) {
				context.abort();
			}
		}
	}

	private void fixPermissions(Context context, Item item) throws SQLException, AuthorizeException {
        // remove read access from the item
        AuthorizeManager.removePoliciesActionFilter(context, item, Constants.READ);
        // and from all bundles and their bitstreams
        for (Bundle bundle : item.getBundles()) {
            AuthorizeManager.removePoliciesActionFilter(context, bundle, Constants.READ);
            for (Bitstream bitstream : bundle.getBitstreams()) {
                AuthorizeManager.removePoliciesActionFilter(context, bitstream, Constants.READ);
            }
        }

        // allow authorised group read access to everything though
        Group authorisedGroup = findCanReadEmbargoedItemsGroup(context);
        if (authorisedGroup != null) {
            AuthorizeManager.addPolicy(context, item, Constants.READ, authorisedGroup);
            for (Bundle bundle : item.getBundles()) {
                AuthorizeManager.addPolicy(context, bundle, Constants.READ, authorisedGroup);
                for (Bitstream bitstream : bundle.getBitstreams()) {
                    AuthorizeManager.addPolicy(context, bitstream, Constants.READ, authorisedGroup);
                }
            }
        }
	}

	private boolean checkPermissions(Context context, Item item) throws SQLException {
        Group authorisedToRead = findCanReadEmbargoedItemsGroup(context);
        boolean policiesOk = true;

        // check for ANY read policies and report them (unless they are in the authorised group):
        for (ResourcePolicy rp : AuthorizeManager.getPoliciesActionFilter(context, item, Constants.READ)) {
            // don't warn for authorised users
            if (rp.getGroupID() != -1 && rp.getGroup().equals(authorisedToRead)) {
                continue;
            }
            // do warn for everyone else
            policiesOk = false;
            report("CHECK WARNING: Item " + item.getHandle() + " allows READ by "
		                   + ((rp.getEPersonID() < 0) ? "Group " + rp.getGroup().getName()
				                      : "EPerson " + rp.getEPerson().getFullName()));
        }

        for (Bundle bn : item.getBundles())
        {
            // check for ANY read policies and report them:
            for (ResourcePolicy rp : AuthorizeManager.getPoliciesActionFilter(context, bn, Constants.READ)) {
                // don't warn for authorised users
                if (rp.getGroupID() != -1 && rp.getGroup().equals(authorisedToRead)) {
                    continue;
                }
                policiesOk = false;
                report("CHECK WARNING: Item " + item.getHandle() + ", Bundle " + bn.getName() + " allows READ by "
		                       + ((rp.getEPersonID() < 0) ? "Group " + rp.getGroup().getName()
				                          : "EPerson " + rp.getEPerson().getFullName()));
            }

            for (Bitstream bs : bn.getBitstreams()) {
                for (ResourcePolicy rp : AuthorizeManager.getPoliciesActionFilter(context, bs, Constants.READ)) {
                    // don't warn for authorised users
                    if (rp.getGroupID() != -1 && rp.getGroup().equals(authorisedToRead)) {
                        continue;
                    }
                    policiesOk = false;
                    report("CHECK WARNING: Item " + item.getHandle() + ", Bitstream " + bs.getName() + " (in Bundle " + bn.getName() + ") allows READ by "
		                           + ((rp.getEPersonID() < 0) ? "Group " + rp.getGroup().getName()
				                              : "EPerson " + rp.getEPerson().getFullName()));
                }
            }
        }
		return policiesOk;
	}


    private static Group findCanReadEmbargoedItemsGroup(Context context) throws SQLException {
	    if (ConfigurationManager.getProperty("lconz.embargo.read.groupid") == null) {
		    return null;
	    }
	    int groupId = ConfigurationManager.getIntProperty("lconz.embargo.read.groupid");
        return Group.find(context, groupId);
    }
}
