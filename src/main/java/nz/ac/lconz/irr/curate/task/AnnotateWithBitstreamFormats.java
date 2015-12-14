package nz.ac.lconz.irr.curate.task;

import org.apache.commons.lang.StringUtils;
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
import org.dspace.curate.Mutative;
import org.dspace.embargo.EmbargoManager;
import org.dspace.eperson.Group;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * DSpace curation task to annotate an item with the mime types of its public bitstreams.
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ IRR project
 */
@Mutative
public class AnnotateWithBitstreamFormats extends AbstractCurationTask {
	private static Logger log = Logger.getLogger(AnnotateWithBitstreamFormats.class);

	private String schema;
	private String element;
	private String qualifier;

	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);

		String mdField = ConfigurationManager.getProperty("mimetype", "field");
		if (mdField == null) {
			mdField = "dc.format.mimetype";
		}
		log.debug("Using mimetype field " + mdField);

		String[] mdComponents = mdField.split("\\.");
		schema = mdComponents[0];
		element = mdComponents[1];
		if (mdComponents.length > 2)
		{
			qualifier = mdComponents[2];
		}

	}

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso.getType() != Constants.ITEM) {
			return Curator.CURATE_SKIP;
		}

		Item item = (Item) dso;

		Context context = null;
		try {
			context = Curator.curationContext();

			List<String> mimetypesFromMetadata = new ArrayList<>();
			Metadatum[] metadata = item.getMetadata(schema, element, qualifier, Item.ANY);
			if (metadata != null) {
				for (Metadatum mdEntry : metadata) {
					mimetypesFromMetadata.add(mdEntry != null ? mdEntry.value : null);
				}
			}

			List<String> mimetypes = getBitstreamFormats(context, item);

			String message;
			if (mimetypesFromMetadata.equals(mimetypes)) {
				message = "Item " + item.getName() + ": mime types unchanged";

				report(message);
				setResult(message);
				log.info(message);

				return Curator.CURATE_SKIP;
			}

			item.clearMetadata(schema, element, qualifier, Item.ANY);

			if (!mimetypes.isEmpty())
			{
				String[] mimeArray = mimetypes.toArray(new String[mimetypes.size()]);
				item.addMetadata(schema, element, qualifier, null, mimeArray);

				message = "Item " + item.getName() + ": mime types " + Arrays.deepToString(mimeArray);
			} else {
				message = "Item " + item.getName() + ": no mime types of public bitstreams found";
			}

			item.update();

			report(message);
			setResult(message);
			log.info(message);

			return Curator.CURATE_SUCCESS;
		} catch (SQLException | AuthorizeException e) {
			String message = "Problem annotating item id=" + item.getID() + " with mime types: " + e.getMessage();
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}
	}

	private List<String> getBitstreamFormats(Context c, Item item) throws SQLException, AuthorizeException, IOException {
		// return empty list if item isn't public
		if (!anonymousCanRead(c, item)) {
			return Collections.emptyList();
		}

		// get 'original' bundles
		Bundle[] bundles = item.getBundles(Constants.CONTENT_BUNDLE_NAME);
		List<String> mimetypes = new ArrayList<>();

		for (Bundle bundle : bundles)
		{
			if (!anonymousCanRead(c, bundle))
			{
				// skip non-public bundle
				continue;
			}

			int primaryId = bundle.getPrimaryBitstreamID();
			// now look at all of the bitstreams
			Bitstream[] bitstreams = bundle.getBitstreams();
			for (Bitstream bitstream : bitstreams)
			{
				if (!anonymousCanRead(c, bitstream))
				{
					// skip non-public bitstream
					continue;
				}

				String mimetype = bitstream.getFormat().getMIMEType();
				if (StringUtils.isNotBlank(mimetype))
				{
					if (bitstream.getID() == primaryId)
					{
						// insert at beginning of list
						mimetypes.add(0, mimetype);
					}
					else
					{
						// just append to list
						mimetypes.add(mimetype);
					}
				}
			}
		}
		return mimetypes;
	}

	private boolean anonymousCanRead(Context c, DSpaceObject dso) throws SQLException {
		ResourcePolicy anonymousReadPolicy = AuthorizeManager.findByTypeIdGroupAction(c, dso.getType(), dso.getID(), Group.ANONYMOUS_ID, Constants.READ, -1);
		return anonymousReadPolicy != null && anonymousReadPolicy.isDateValid();
	}
}
