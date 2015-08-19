package nz.ac.lconz.irr.curate.task;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.Metadatum;
import org.dspace.core.ConfigurationManager;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the UoW Institutional Research Repositories
 */
@Distributive
public class DistributiveUriLinkChecker extends AbstractCurationTask {
	private Map<String, Integer> statusCache;
	private int numChecked;
	private int numOk;
	private int numNotOk;

	private boolean checkSelfLinks;
	private String handlePrefix;
	private String userAgent;

	private static Logger log = Logger.getLogger(DistributiveUriLinkChecker.class);

	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);
		checkSelfLinks = taskBooleanProperty("selflinks.check", false);
		if (!checkSelfLinks) {
			handlePrefix = ConfigurationManager.getProperty("handle.canonical.prefix") + ConfigurationManager.getProperty("handle.prefix");
		}
		statusCache = new HashMap<>();
		userAgent = String.format("DSpace link checker for %s", ConfigurationManager.getProperty("dspace.url"));
	}

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		numChecked = 0;
		numOk = 0;
		numNotOk = 0;

		distribute(dso);

		String message = String.format("Checked %d links; %d were ok and %d weren't.", numChecked, numOk, numNotOk);
		report(message);
		setResult(message);

		if (numChecked == 0) {
			return Curator.CURATE_SKIP;
		}
		if (numNotOk > 0) {
			return Curator.CURATE_FAIL;
		}
		return Curator.CURATE_SUCCESS;
	}

	@Override
	protected void performItem(Item item) throws SQLException, IOException {
		List<String> urls = getURLs(item, checkSelfLinks, handlePrefix);
		StringBuilder itemResults = new StringBuilder();
		itemResults.append("Item: ").append(getItemHandle(item)).append("\n");

		for (String url : urls) {
			boolean success = checkURL(url, itemResults, statusCache);
			if (success) {
				numOk++;
			} else {
				numNotOk++;
			}
			numChecked++;
		}
		report(itemResults.toString());
	}


	/**
	 * Check the URL and perform appropriate reporting
	 *
	 * @param url The URL to check
	 * @return If the URL was OK or not
	 */
	protected boolean checkURL(String url, StringBuilder results, Map<String, Integer> statusCache)
	{
		// Link check the URL
		int httpStatus;
		if (statusCache != null && statusCache.containsKey(url)) {
			httpStatus = statusCache.get(url);
		} else {
			httpStatus = getResponseStatus(url);
			if (statusCache != null) {
				statusCache.put(url, httpStatus);
			}
		}

		if ((httpStatus >= 200) && (httpStatus < 300))
		{
			results.append(" - ").append(url).append(" = ").append(httpStatus).append(" - OK\n");
			return true;
		}
		else
		{
			results.append(" - ").append(url).append(" = ").append(httpStatus).append(" - FAILED\n");
			return false;
		}
	}

	/**
	 * Get the response code for a URL.  If something goes wrong opening the URL, a
	 * response code of 0 is returned.
	 *
	 * @param url The url to open
	 * @return The HTTP response code (e.g. 200 / 301 / 404 / 500) or 0 if an exception was encountered
	 */
	protected int getResponseStatus(String url)
	{
		try
		{
			URL theURL = new URL(url);
			HttpURLConnection connection = (HttpURLConnection)theURL.openConnection();
			connection.setRequestProperty("User-Agent", userAgent);
			int code = connection.getResponseCode();
			connection.disconnect();

			return code;

		}
		catch (IOException | RuntimeException e)
		{
			// Must be a bad URL
			log.info("Encountered exception while trying to check url (" + url + "): " + e.getMessage());
			return 0;
		}
	}

	protected List<String> getURLs(Item item, boolean includeSelfLinks, String handlePrefix) {
		// Get URIs from anyschema.anyelement.uri.*
		Metadatum[] urls = item.getMetadata(Item.ANY, Item.ANY, "uri", Item.ANY);
		ArrayList<String> theURLs = new ArrayList<>();
		for (Metadatum url : urls)
		{
			// unless we include self links, only add those URLs NOT starting with the handle prefix
			if (includeSelfLinks || !StringUtils.startsWithIgnoreCase(url.value, handlePrefix)) {
				theURLs.add(url.value);
			}
		}
		return theURLs;
	}

	/**
	 * Internal utitity method to get a description of the handle
	 *
	 * @param item The item to get a description of
	 * @return The handle, or in workflow
	 */
	private static String getItemHandle(Item item)
	{
		String handle = item.getHandle();
		return (handle != null) ? handle: " in workflow, item_id=" + item.getID();
	}
}
