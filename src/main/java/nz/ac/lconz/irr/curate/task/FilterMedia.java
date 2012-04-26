package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.dspace.app.mediafilter.MediaFilterManager;
import org.dspace.content.Collection;
import org.dspace.content.Community;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: schweer
 * Date: 26/04/12
 * Time: 4:41 PM
 * To change this template use File | Settings | File Templates.
 */
@Distributive
public class FilterMedia extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(FilterMedia.class);

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso.getType() == Constants.BITSTREAM || dso.getType() == Constants.BUNDLE) {
			return Curator.CURATE_SKIP; // must be at least an item
		}
		String handle = dso.getHandle();
		if (handle == null || "".equals(handle)) {
			return Curator.CURATE_SKIP; // need handle
		}

		log.info("Running media filters on object with handle=" + handle);

		try {
			Context context = new Context();
			context.ignoreAuthorization();

			String message = "";

			switch (dso.getType()) {
				case Constants.ITEM:
					Item item = (Item) dso;
					MediaFilterManager.applyFiltersItem(context, item);
					message = "Ran media filters on item " + item.getName() + " (handle " + handle + ")";
					break;
				case Constants.COLLECTION:
					Collection collection = (Collection) dso;
					MediaFilterManager.applyFiltersCollection(context, collection);
					message = "Ran media filters on collection " + collection.getName() + " (handle " + handle + ")";
					break;
				case Constants.COMMUNITY:
					Community community = (Community) dso;
					MediaFilterManager.applyFiltersCommunity(context, community);
					message = "Ran media filters on community " + community.getName() + " (handle " + handle + ")";
					break;
				case Constants.SITE:
					MediaFilterManager.applyFiltersAllItems(context);
					message = "Ran media filters on all items";
					break;
			}
			context.complete();
			setResult(message);
			report(message);
			return Curator.CURATE_SUCCESS;
		} catch (Exception e) {
			String message = "Problem running media filter curation task: " + e.getMessage();
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}
	}
}
