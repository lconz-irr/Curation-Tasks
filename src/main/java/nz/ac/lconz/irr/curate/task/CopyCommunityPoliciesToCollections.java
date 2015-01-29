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
import org.dspace.curate.Distributive;
import org.dspace.curate.Mutative;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
@Distributive
@Mutative
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
			context = Curator.curationContext();

			workOnCommunity(context, community);

			context.commit();

			String message = String.format("Child communities/collections inherited the policies of the community \"%s\"", community.getName());
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
		}
	}

	private void workOnCommunity(Context context, Community community) throws SQLException, AuthorizeException {
		Collection[] collections = community.getCollections();
		for (Collection collection : collections) {
			inheritWithoutDuplicates(context, community, collection);
		}

		Community[] children = community.getSubcommunities();
		for (Community child : children) {
			inheritWithoutDuplicates(context, community, child);
			workOnCommunity(context, child);
		}
	}

	private void inheritWithoutDuplicates(Context context, Community community, DSpaceObject target) throws SQLException, AuthorizeException {
		AuthorizeManager.inheritPolicies(context, community, target);

		// then remove duplicate policies
		List<ResourcePolicy> policies = AuthorizeManager.getPolicies(context, target);
		Set<ResourcePolicy> alreadySeen = new HashSet<ResourcePolicy>();
		for (ResourcePolicy policy : policies) {
			if (alreadySeen.contains(policy)) {
				policy.delete();
			} else {
				alreadySeen.add(policy);
			}
		}
	}

}
