package nz.ac.lconz.irr.utils;

import nz.ac.lconz.irr.curate.task.ImageMagickThumbnailer;
import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeManager;
import org.dspace.content.DSpaceObject;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.eperson.Group;

import java.sql.SQLException;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for LCoNZ
 */
public class AuthorizeUtils {

	private static Logger log = Logger.getLogger(AuthorizeUtils.class);

	public static boolean anonymousCanRead(DSpaceObject dso) {
		Context context = null;
		try {
			context = new Context();
			Group[] readGroups = AuthorizeManager.getAuthorizedGroups(context, dso, Constants.READ);
			for (Group group : readGroups) {
				if (group.getID() == 0) {
					return true;
				}
			}
		} catch (SQLException e) {
			log.error("Cannot determine whether object is public (id=" + dso.getID() + ")", e);
		} finally {
			if (context != null) {
				context.abort();
			}
		}
		return false;
	}
}
