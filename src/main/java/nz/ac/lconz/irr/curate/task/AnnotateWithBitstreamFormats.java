package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.AuthorizeManager;
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
		log.info("Using mimetype field " + mdField);

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

		// TODO make distributive to avoid creating lots of contexts?
		Context context = null;
		try {
			context = new Context();
			context.ignoreAuthorization();

			boolean changes;

			changes = clearExisting(item);
			List<String> mimetypes = getBitstreamFormats(context, item);

			String message;
			if (!mimetypes.isEmpty())
			{
				String[] mimeArray = mimetypes.toArray(new String[mimetypes.size()]);
				item.addMetadata(schema, element, qualifier, null, mimeArray);
				changes = true;

				setResult(Arrays.deepToString(mimeArray));
				message = "Item " + item.getName() + ": mime types " + Arrays.deepToString(mimeArray);
			} else {
				message = "Item " + item.getName() + ": no mime types of public bitstreams found";
			}

			report(message);
			setResult(message);
			log.info(message);

			if (changes) {
				item.update();
				context.complete();
				context = null;
				return Curator.CURATE_SUCCESS;
			} else {
				context.abort();
				context = null;
				return Curator.CURATE_SKIP;
			}
		} catch (SQLException e) {
			String message = "Problem annotating item id=" + item.getID() + " with mime types: " + e.getMessage();
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} catch (AuthorizeException e) {
			String message = "Problem annotating item id=" + item.getID() + " with mime types: " + e.getMessage();
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} finally {
			if (context != null) {
				context.abort();
			}
		}
	}

	private List<String> getBitstreamFormats(Context c, Item item) throws SQLException, AuthorizeException, IOException {
		// get 'original' bundles
		Bundle[] bundles = item.getBundles("ORIGINAL");
		List<String> mimetypes = new ArrayList<String>();

        //check for embargo and return empty list if item has an embargo
        DCDate embargoDate = EmbargoManager.getEmbargoDate(c, item);
        if(embargoDate != null){
            return mimetypes;
        }

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
				if (mimetype != null && ! "".equals(mimetype))
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

	private boolean clearExisting(Item item) {
		boolean changes = false;
		DCValue[] existingMetadata = item.getMetadata(schema, element, qualifier, Item.ANY);
		if (existingMetadata != null && existingMetadata.length > 0)
		{
				item.clearMetadata(schema, element, qualifier, Item.ANY);
				changes = true;
		}
		return changes;
	}

	private boolean anonymousCanRead(Context c, DSpaceObject dso) throws SQLException {
		Group[] readGroups = AuthorizeManager.getAuthorizedGroups(c, dso, Constants.READ);
		for (Group group : readGroups)
		{
			if (group.getID() == 0)
			{
				return true;
			}
		}
		return false;
	}
}
