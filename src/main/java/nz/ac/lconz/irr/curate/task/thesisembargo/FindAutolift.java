package nz.ac.lconz.irr.curate.task.thesisembargo;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeManager;
import org.dspace.authorize.ResourcePolicy;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.*;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;
import org.dspace.handle.HandleManager;

import javax.mail.MessagingException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
@Distributive
public class FindAutolift extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(FindAutolift.class);

	private int numFound;

	private StringBuilder messageText;
	private List<String> recipients = null;
	private boolean sendIfEmpty;
	private String templateName;
	private Map<String, List<String>> found;

	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);
		String recipientsProperty = taskProperty("recipients");
		if (recipientsProperty != null && !"".equals(recipientsProperty)) {
			String[] recipientsList = recipientsProperty.split(",\\s*");
			recipients = Arrays.asList(recipientsList);
		}
		sendIfEmpty = taskBooleanProperty("email.send-if-empty", false);
		templateName = taskProperty("template");
		if (templateName == null || "".equals(templateName)) {
			templateName = "autolift_items_email";
		}
	}

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		numFound = 0;
		messageText = new StringBuilder();
		found = new HashMap<String, List<String>>();

		distribute(dso);

		String resultText = String.format("%d item(s) found with policies that will automatically apply/cease to apply at a future date", numFound);
		report(resultText);
		messageText.append(resultText);

		boolean hasError = false;

		if (recipients != null && !recipients.isEmpty()) {
			if (numFound > 0 || sendIfEmpty) {
				Email email = Email.getEmail(I18nUtil.getEmailFilename(Locale.getDefault(), templateName));
				for (String recipient : recipients) {
					email.addRecipient(recipient);
				}

				email.addArgument(messageText.toString());

				try {
					email.send();
					setResult(resultText + "; e-mail sent.");
				} catch (MessagingException e) {
					String error = "Cannot send e-mail: " + e.getMessage();
					log.error(error, e);
					report(error);
					setResult(error);
					hasError = true;
				} catch (IOException e) {
					String error = "Cannot send e-mail: " + e.getMessage();
					log.error(error, e);
					report(error);
					setResult(error);
					hasError = true;
				}
			}
		}

		messageText = null;
		found = null;

		if (hasError) {
			return Curator.CURATE_ERROR;
		}
		if (numFound > 0) {
			return Curator.CURATE_SUCCESS;
		} else {
			return Curator.CURATE_SKIP;
		}
	}

	@Override
	protected void performItem(Item item) throws SQLException, IOException {
		if (!item.isArchived()) {
			return; // do nothing
		}

		boolean hasAutolift = checkAutolift(item, item.getHandle());
		hasAutolift |= checkBundlesBitstreamsAutolift(item);

		if (hasAutolift) {
			numFound++;
			StringBuilder message = new StringBuilder(HandleManager.resolveToURL(Curator.curationContext(), item.getHandle())).append("\n");
			List<String> details = found.get(item.getHandle());
			for (String detail : details) {
				message.append(detail).append("\n");
			}
			report(message.toString());
			messageText.append(message).append("\n");
		}
	}

	private boolean checkBundlesBitstreamsAutolift(Item item) throws SQLException {
		boolean autoliftFound = false;
		Bundle[] bundles = item.getBundles();
		for (Bundle bundle : bundles) {
			autoliftFound |= checkAutolift(bundle, item.getHandle());
			Bitstream[] bitstreams = bundle.getBitstreams();
			for (Bitstream bitstream : bitstreams) {
				autoliftFound |= checkAutolift(bitstream, item.getHandle());
			}
		}
		return autoliftFound;
	}

	private boolean checkAutolift(DSpaceObject dso, String parentHandle) throws SQLException {
		Date now = new Date();
		boolean autoliftFound = false;
		List<ResourcePolicy> policies = AuthorizeManager.getPolicies(Curator.curationContext(), dso);
		for (ResourcePolicy policy : policies) {
			Date startDate = policy.getStartDate();
			Date endDate = policy.getEndDate();
			if ((endDate != null && endDate.after(now)) || (startDate != null && startDate.after(now))) {
				autoliftFound = true;
				if (!found.containsKey(parentHandle)) {
					found.put(parentHandle, new ArrayList<String>());
				}
				found.get(parentHandle).add("From " + startDate + (endDate != null ? " to " + endDate : "") + ": " + policy.getActionText().toLowerCase() + " " + dso.getTypeText().toLowerCase() + " " + dso.getName());
			}
		}
		return autoliftFound;
	}

}
