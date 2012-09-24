package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @author Andrea Schweer for the LCoNZ Institutional Research Repositories
 *
 * Curation task to make a bitstream with the name thesis.pdf the primary bitstream in the content bundle.
 * It will also re-order the bitstreams in the content bundle to ensure that the primary bitstream is first in the bitstream order.
 */
public class MakeThesisPrimaryBitstream extends AbstractCurationTask {
	private final static Logger log = Logger.getLogger(MakeThesisPrimaryBitstream.class);


	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso.getType() != Constants.ITEM) {
			return Curator.CURATE_SKIP;
		}
		Item item = (Item) dso;

		Bitstream thesisBitstream = null;
		Bundle thesisBundle = null;

		try {
			Bundle[] contentBundles = item.getBundles(Constants.CONTENT_BUNDLE_NAME);
			for (Bundle bundle : contentBundles) {
				if (bundle.getPrimaryBitstreamID() > 0) {
					String message = "Item id=" + item.getID() + " already has a primary bitstream";
					log.info(message);
					report(message);
					setResult(message);
					return Curator.CURATE_SKIP;
				}

				Bitstream thesisCandidate = bundle.getBitstreamByName("thesis.pdf");
				if (thesisCandidate != null) {
					thesisBitstream = thesisCandidate;
					thesisBundle = bundle;
					break;
				}
			}
		} catch (SQLException e) {
			String message = "Problem getting bundles for item id=" + item.getID();
			log.warn(message, e);
			report(message);
			setResult(e.getMessage());
			return Curator.CURATE_ERROR;
		}

		if (thesisBitstream != null && thesisBundle != null) {
			try {
				thesisBundle.setPrimaryBitstreamID(thesisBitstream.getID());
				thesisBundle.update();

				makePrimaryBitstreamFirst(thesisBundle);
				thesisBundle.update();
			} catch (SQLException e) {
				String message = "Problem setting primary bitstream id=" + thesisBitstream.getID() + " for item id=" + item.getID();
				log.warn(message, e);
				report(message);
				setResult(e.getMessage());
				return Curator.CURATE_ERROR;
			} catch (AuthorizeException e) {
				String message = "Not authorised to set primary bitstream id=" + thesisBitstream.getID() + " for item id=" + item.getID();
				log.warn(message, e);
				report(message);
				setResult(e.getMessage());
				return Curator.CURATE_ERROR;
			}
			String message = "Set primary bitstream id=" + thesisBitstream.getID() + " for item id=" + item.getID();
			setResult(message);
			report(message);
			log.info(message);
			return Curator.CURATE_SUCCESS;
		} else {
			String message = "No thesis.pdf file found for item " + item.getID();
			setResult(message);
			report(message);
			log.info(message);
			return Curator.CURATE_FAIL;
		}
	}

	private void makePrimaryBitstreamFirst(Bundle bundle) throws AuthorizeException, SQLException {
		int primaryBitstreamID = bundle.getPrimaryBitstreamID();
		if (primaryBitstreamID < 0) {
			return; // no primary bitstream -> nothing to be done
		}

		Bitstream[] bitstreams = bundle.getBitstreams();
		ArrayList<Integer> ids = new ArrayList<Integer>(bitstreams.length);

		for (Bitstream bitstream : bitstreams) {
			int bitstreamID = bitstream.getID();
			if (bitstreamID == primaryBitstreamID) {
				ids.add(0, bitstreamID); // insert as first
			} else {
				ids.add(bitstreamID); // insert in order
			}
		}
		int[] newOrder = new int[ids.size()];
		for (int i = 0; i < ids.size(); i++) {
			newOrder[i] = ids.get(i);
		}
		bundle.setOrder(newOrder);
	}
}
