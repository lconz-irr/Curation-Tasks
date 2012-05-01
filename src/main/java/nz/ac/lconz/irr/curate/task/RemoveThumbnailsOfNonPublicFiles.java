package nz.ac.lconz.irr.curate.task;

import nz.ac.lconz.irr.utils.AuthorizeUtils;
import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for LCoNZ
 */
public class RemoveThumbnailsOfNonPublicFiles extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(RemoveThumbnailsOfNonPublicFiles.class);

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso.getType() != Constants.ITEM) {
			return Curator.CURATE_SKIP;
		}

		Item item = (Item) dso;

		try {
			int bitstreamsRemoved = 0;
			int bundlesRemoved = 0;

			boolean itemPublic = AuthorizeUtils.anonymousCanRead(item);
			Bundle[] thumbnailBundles = item.getBundles("THUMBNAIL");
			for (Bundle bundle : thumbnailBundles) {
				boolean bundlePublic = AuthorizeUtils.anonymousCanRead(bundle);
				Bitstream[] bitstreams = bundle.getBitstreams();
				for (Bitstream bitstream : bitstreams) {
					boolean bitstreamPublic = AuthorizeUtils.anonymousCanRead(bitstream);
					if (!bitstreamPublic || !bundlePublic || !itemPublic) {
						bundle.removeBitstream(bitstream);
						report("Removing bitstream " + bitstream.getName() + " from bundle. Item id=" + item.getID());
						bitstreamsRemoved++;
					}
				}
				if (!bundlePublic || !itemPublic) {
					item.removeBundle(bundle);
					report("Removing thumbnail bundle from item id=" + item.getID());
					bundlesRemoved++;
				}
			}

			if (bitstreamsRemoved > 0 || bundlesRemoved > 0) {
				StringBuilder messageBuilder = new StringBuilder("Removed thumbnails for non-public files in item ");
				messageBuilder.append(item.getName());
				messageBuilder.append(": removed ");
				messageBuilder.append(bitstreamsRemoved);
				messageBuilder.append(" bitstreams and ");
				messageBuilder.append(bundlesRemoved);
				messageBuilder.append(" bundles.");
				String message = messageBuilder.toString();

				report(message);
				setResult(message);
				log.info(message);
				return Curator.CURATE_SUCCESS;
			} else {
				String message = "No thumbnails of non-public files to remove for item " + item.getName();
				report(message);
				setResult(message);
				return Curator.CURATE_FAIL;
			}
		} catch (SQLException e) {
			String message = "Problem removing thumbnails of non-public files: " + e.getMessage();
			report(message);
			setResult(message);
			log.error(message, e);
			return Curator.CURATE_ERROR;
		} catch (AuthorizeException e) {
			String message = "Problem removing thumbnails of non-public files: " + e.getMessage();
			report(message);
			setResult(message);
			log.error(message, e);
			return Curator.CURATE_ERROR;
		}
	}
}
