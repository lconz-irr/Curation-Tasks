package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.dspace.content.Collection;
import org.dspace.content.Community;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;
import org.dspace.statistics.SolrLogger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Curation task to update community/collection information for item page views and bitstream downloads
 * in the usage statistics. This task ensures that the owningColl and owningComm information in the usage statistics
 * matches the actual collections/communities that the item belongs to.
 *
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
@Distributive
public class UpdateStatsContainers extends AbstractCurationTask {
	private static Logger log = Logger.getLogger(UpdateStatsContainers.class);
	private int numProcessedItems;
	private int numErrorItems;

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		numProcessedItems = 0;
		numErrorItems = 0;
		try {
			distribute(dso);
		} catch (IOException e) {
			String message = "Problem processing items: " + e.getMessage();
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}
		formatResults();
		if (numProcessedItems > 0) {
			return Curator.CURATE_SUCCESS;
		} else {
			return Curator.CURATE_SKIP;
		}
	}

	private void formatResults() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(numProcessedItems);
		buffer.append(" items processed. \n");
		if (numErrorItems > 0) {
			buffer.append(numErrorItems);
			buffer.append(" with errors. \n");
		}
		buffer.append("Any changes may take a while to show up due to solr auto-commit settings.\n");

		String result = buffer.toString();
		report(result);
		setResult(result);
	}

	/**
	 * Performs task upon a single DSpace Item. Used in conjunction with the
	 * <code>distribute</code> method to run a single task across multiple Items.
	 * <p/>
	 * You should override this method if you want to use
	 * <code>distribute</code> to run your task across multiple DSpace Items.
	 * <p/>
	 * Either this method or <code>performObject</code> should be overridden if
	 * <code>distribute</code> method is used.
	 *
	 * @param item the DSpace Item
	 * @throws java.sql.SQLException
	 * @throws java.io.IOException
	 */
	@Override
	protected void performItem(Item item) throws SQLException, IOException {
		List<String> fieldsToChange = Arrays.asList(new String[]{"owningColl", "owningComm"});
		ArrayList<List<Object>> newValues = new ArrayList<List<Object>>();

		ArrayList<Object> newCollections = new ArrayList<Object>();
		Collection[] collections = item.getCollections();
		for (Collection coll : collections) {
			newCollections.add(coll.getID());
		}
		newValues.add(newCollections);

		ArrayList<Object> newCommunities = new ArrayList<Object>();
		Community[] communities = item.getCommunities();
		for (Community comm : communities) {
			newCommunities.add(comm.getID());
		}
		newValues.add(newCommunities);

		try {
			SolrLogger.update("(type:" + Constants.ITEM + " AND id:" + item.getID()
					                  + ") OR (type:" + Constants.BITSTREAM + " AND owningItem:" + item.getID() + ")",
					                 "replace",
					                 fieldsToChange,
					                 newValues);
		} catch (SolrServerException e) {
			numErrorItems++;
			report("Problem processing item id=" + item.getID() + ": " + e.getMessage());
			log.error("Problem updating collection/community info in usage stats for item id=" + item.getID() + ": " + e.getMessage(), e);
		}
		report("Processed item id=" + item.getID());
		numProcessedItems++;
	}
}
