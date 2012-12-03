package nz.ac.lconz.irr.curate.task;

import org.dspace.authorize.AuthorizeException;
import org.dspace.authorize.AuthorizeManager;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.content.Collection;
import org.dspace.content.Community;
import org.dspace.content.DSpaceObject;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class CopyCommunityPoliciesToCollections extends AbstractCurationTask {
	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso == null || dso.getType() != Constants.COMMUNITY) {
			String message = "This curation task must be run on a community";
			report(message);
			setResult(message);
			return Curator.CURATE_SKIP;
		}
		Community community = (Community) dso;
		Context context = null;
		try {
			context = new Context();
			Collection[] collections = community.getCollections();

			for (Collection collection : collections) {
				AuthorizeManager.inheritPolicies(context, community, collection);
			}

			context.commit();
			context = null;

			String message = String.format("%d child collections inherited the policies of the community\"%d\"", collections.length, community.getName());
			report(message);
			setResult(message);

			return Curator.CURATE_SUCCESS;
		} catch (SQLException e) {
			String message = e.getMessage();
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} catch (AuthorizeException e) {
			String message = e.getMessage();
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} finally {
			if (context != null) {
				context.abort();
			}
		}
	}

}
