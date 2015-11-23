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
import org.dspace.curate.Mutative;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
@Distributive
@Mutative
public class ExtractPermissionsMatchOriginal extends AbstractCurationTask {
	private static Logger log = Logger.getLogger(ExtractPermissionsMatchOriginal.class);

	private String[] derivedBundleNames;

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso == null) {
			return Curator.CURATE_SKIP;
		}

		initBundleNames();

		if (derivedBundleNames == null) {
			String message = "No derived bundles configured, aborting";
			log.warn(message);
			report(message);
			return Curator.CURATE_FAIL;
		}

		Context context;

		boolean changes = false;
		try {
			context = Curator.curationContext();

			if (dso.getType() == Constants.SITE) {
				ItemIterator items = Item.findAll(context);
				changes = processItems(context, items);
			} else if (dso.getType() == Constants.COMMUNITY) {
				Community community = (Community) dso;
				Collection[] collections = community.getCollections();
				for (Collection collection : collections) {
					changes |= workOnCollection(collection, context);
				}
			} else if (dso.getType() == Constants.COLLECTION) {
				Collection collection = (Collection) dso;
				changes = workOnCollection(collection, context);
			} else if (dso.getType() == Constants.ITEM) {
				Item item = (Item) dso;
				try {
					changes = workOnItem(item, context);
				} catch (RuntimeException e) {
					String message = "Problem while performing task on item id=" + item.getID() + ", " + item.getType() + "; " + e.getMessage();
					log.error(message, e);
					report(message);
				}
			} else {
				return Curator.CURATE_SKIP;
			}

			if (changes) {
				context.commit();
			}
		} catch (SQLException | AuthorizeException e) {
			String message = "Problem while performing task on object id=" + dso.getID() + ", " + dso.getType() + "; " + e.getMessage();
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}

		if (changes) {
			String message = "Matched policies for object id=" + dso.getID() + ", " + dso.getName();
			report(message);
			setResult(message);
			return Curator.CURATE_SUCCESS;
		} else {
			String message = "No changes made to object id=" + dso.getID() + ", " + dso.getName();
			report(message);
			setResult(message);
			return Curator.CURATE_FAIL;
		}
	}

	private boolean processItems(Context context, ItemIterator items) throws SQLException, AuthorizeException, IOException {
		boolean changes = false;
		while (items.hasNext()) {
            Item item = items.next();
            try {
                changes |= workOnItem(item, context);
            } catch (RuntimeException e) {
                String message = "Problem while performing task on item id=" + item.getID() + ", " + item.getType() + "; " + e.getMessage();
                log.error(message, e);
                report(message);
            }
        }
		return changes;
	}

	private boolean workOnCollection(Collection collection, Context context) throws SQLException, AuthorizeException, IOException {
		ItemIterator items = collection.getItems();
		return processItems(context, items);
	}

	private boolean workOnItem(Item item, Context context) throws SQLException, AuthorizeException, IOException {
		boolean changes = false;
		Bundle originalBundle = null;
		Bundle[] originalBundles = item.getBundles(Constants.CONTENT_BUNDLE_NAME);
		if (originalBundles != null && originalBundles.length > 0) {
			originalBundle = originalBundles[0];
		}

		for (String derivedBundleName : derivedBundleNames) {
			Bundle[] bundles = item.getBundles(derivedBundleName);
			for (Bundle bundle : bundles) {
				if (originalBundle == null) {
					if (deleteIfNoOriginal()) {
						report("Bundle id=" + bundle.getID() + " doesn't have an original, deleting");
						item.removeBundle(bundle);
						changes = true;
					} else {
						report("Bundle id=" + bundle.getID() + " doesn't have an original, but not deleting according to settings");
					}
				}

				changes |= matchPolicies(context, originalBundle, bundle);

				Bitstream[] bitstreams = bundle.getBitstreams();
				for (Bitstream bitstream : bitstreams) {
					Bitstream originalBitstream = findOriginal(originalBundles, bitstream);
					if (originalBitstream == null) {
						if (deleteIfNoOriginal()) {
							report("Bitstream id=" + bitstream.getID() + " (" + bitstream.getName() + ") doesn't have an original, deleting");
							bundle.removeBitstream(bitstream);
							changes = true;
						} else {
							report("Bitstream id=" + bitstream.getID() + " (" + bitstream.getName() + ") doesn't have an original, but not deleting according to settings");
						}
					} else {
						changes |= matchPolicies(context, originalBitstream, bitstream);
					}
				}
			}
		}
		if (changes) {
			context.commit();
		}
		return changes;
	}

	private Bitstream findOriginal(Bundle[] originalBundles, Bitstream derivative) {
		String name = derivative.getName();
		String nameWithoutSuffix = name.substring(0, name.lastIndexOf("."));

		for (Bundle bundle : originalBundles) {
			Bitstream candidate = bundle.getBitstreamByName(nameWithoutSuffix);
			if (candidate != null) {
				return candidate;
			}
		}
		return null;
	}

	private boolean matchPolicies(Context context, DSpaceObject source, DSpaceObject target) throws SQLException, AuthorizeException {
		List<ResourcePolicy> sourcePolicies = AuthorizeManager.getPolicies(context, source);
		List<ResourcePolicy> targetPolicies = AuthorizeManager.getPolicies(context, target);
		if (sourcePolicies.equals(targetPolicies)) {
			report("policies of target object " + target.getName() + " already match those of source " + source.getName() + "; no change needed");
			return false;
		}

		AuthorizeManager.removeAllPolicies(context, target);
		AuthorizeManager.inheritPolicies(context, source, target);

		report("made policies of target object " + target.getID() + " " + target.getName() + " match those of source " + source.getID() + " " + source.getName());

		return true;
	}

	private boolean deleteIfNoOriginal() {
		return taskBooleanProperty("no-original.delete", false);
	}

	private void initBundleNames() {
		if (derivedBundleNames != null) {
			return;
		}
		String namesProperty = ConfigurationManager.getProperty("webui.derived.bundles");
		if (namesProperty != null && !"".equals(namesProperty)) {
			derivedBundleNames = namesProperty.split(",\\s*");
		}

		if (derivedBundleNames == null) {
			derivedBundleNames = new String[] {"TEXT", "THUMBNAIL"};
			log.info("No derived bundle names configured, using default: " + Arrays.deepToString(derivedBundleNames));
		}
	}
}
