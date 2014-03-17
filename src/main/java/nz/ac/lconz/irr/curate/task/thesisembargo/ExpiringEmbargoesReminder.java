package nz.ac.lconz.irr.curate.task.thesisembargo;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.log4j.Logger;
import org.dspace.content.DCDate;
import org.dspace.content.DCValue;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Email;
import org.dspace.core.I18nUtil;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Distributive;
import org.dspace.handle.HandleManager;

import javax.mail.MessagingException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

/**
 * Curation task to send an e-mail with a list of expiring and/or expired thesis embargoes.
 *
 * Task options:
 * - field.date: Metadata field that contains the embargo expiry date
 * - include.expired: true|false Whether to include items whose embargo has already expired. Default: true.
 * - include.not-archived: true|false Whether to include items that are not archived (ie, workflow items). Default: false.
 * - lookahead-months: The number of months to look ahead for expiry (-1=no limit; 0=end of last month; 1= end of current month). Default: 0.
 * - recipients: A comma-separated list of e-mail addresses that should receive the e-mail. Default: value of mail.admin property.
 * - email.send-if-empty: true|false Whether to send an e-mail if no items match the criteria. Default: true.
 * - template: The name of the e-mail template (in [dspace]/config/emails). Default: thesisembargo_email.
 *
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
@Distributive
public class ExpiringEmbargoesReminder extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(ExpiringEmbargoesReminder.class);

	private String dateSchema;
	private String dateElement;
	private String dateQualifier;

	private boolean includeExpired;
	private boolean archivedOnly;

	private List<String> recipients;
	private boolean sendIfEmpty;
	private int lookaheadMonths;
	private String templateName;

	private List<EmbargoedItemInfo> items;

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		// reset status
		items = new ArrayList<EmbargoedItemInfo>();

		// do work
		distribute(dso);

		Collections.sort(items, new EmbargoedItemInfoComparator());

		// communicate results
		String message = String.format("Found %d embargoed items", items.size());
		report(message);
		setResult(message);

		boolean hasError = false;

		if ((sendIfEmpty || !items.isEmpty()) && !recipients.isEmpty()) {
			// send e-mail
			Email email = Email.getEmail(I18nUtil.getEmailFilename(Locale.getDefault(), templateName));
			for (String recipient : recipients) {
				email.addRecipient(recipient);
			}

			email.addArgument(buildMessage());

			try {
				email.send();
			} catch (MessagingException e) {
				String error = "Cannot send notification e-mail: " + e.getMessage();
				log.error(error, e);
				report(error);
				setResult(error);
				hasError = true;
			} catch (IOException e) {
				String error = "Cannot send notification e-mail: " + e.getMessage();
				log.error(error, e);
				report(error);
				setResult(error);
				hasError = true;
			}
		}

		items = null;

		if (hasError) {
			return Curator.CURATE_ERROR;
		} else {
			return Curator.CURATE_SUCCESS;
		}
	}
	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);

		String dateField = taskProperty("field.date");
		if (dateField == null || "".equals(dateField)) {
			log.warn("No embargo date field set up");
			return;
		}
		String[] dateFieldParts = dateField.split("\\.");
		if (dateFieldParts.length < 2) {
			log.warn("Invalid value for embargo date field, must be schema.element or schema.element.qualifier");
			return;
		}
		dateSchema = dateFieldParts[0];
		dateElement = dateFieldParts[1];
		if (dateFieldParts.length > 2) {
			dateQualifier = dateFieldParts[2];
		}

		includeExpired = taskBooleanProperty("include.expired", true);
		archivedOnly = !taskBooleanProperty("include.not-archived", false);
		lookaheadMonths = taskIntProperty("lookahead-months", 0);

		if (lookaheadMonths < 0 && !includeExpired) {
			log.warn("Embargo e-mails don't include expired items but lookahead window is negative - contradictory configuration, no items can match these criteria");
		}

		String recipientsProperty = taskProperty("recipients");
		if (recipientsProperty != null && !"".equals(recipientsProperty)) {
			String[] recipientsList = recipientsProperty.split(",\\s*");
			recipients = Arrays.asList(recipientsList);
		} else {
			recipients = new ArrayList<String>();
			recipients.add(ConfigurationManager.getProperty("mail.admin"));
		}

		sendIfEmpty = taskBooleanProperty("email.send-if-empty", false);
		templateName = taskProperty("template");
		if (templateName == null || "".equals(templateName)) {
			templateName = "thesisembargo_email";
		}
	}

	@Override
	protected void performItem(Item item) throws SQLException, IOException {
		if (item.isWithdrawn()) {
			return; // don't count withdrawn items
		}
		if (archivedOnly && !item.isArchived()) {
			return; // we don't want non-archived items
		}

		DCValue[] dateMetadata = item.getMetadata(dateSchema, dateElement, dateQualifier, Item.ANY);
		if (dateMetadata == null || dateMetadata.length == 0) {
			return; // item isn't embargoed
		}
		DCDate embargoDate = new DCDate(dateMetadata[0].value);
		if (embargoDate.toDate() == null) {
			return;  // item isn't embargoed
		}

		boolean include = true;
		if (!includeExpired) {
			include = !checkExpired(embargoDate);
		}
		if (lookaheadMonths >= 0) {
			include &= checkExpiresWithinMonths(embargoDate, lookaheadMonths);
		}
		if (include) {
			items.add(new EmbargoedItemInfo(item, embargoDate));
			report(embargoDate.toString() + ": " + item.getHandle());
		}
	}

	private boolean checkExpiresWithinMonths(DCDate liftDate, int lookaheadMonths) {
		Calendar beginningOfMonth = Calendar.getInstance(Locale.getDefault());
		beginningOfMonth.setTime(new Date());
		// wind back to beginning of month
		beginningOfMonth.set(Calendar.DAY_OF_MONTH, beginningOfMonth.getMinimum(Calendar.DAY_OF_MONTH));
		beginningOfMonth.set(Calendar.HOUR_OF_DAY, beginningOfMonth.getMinimum(Calendar.HOUR_OF_DAY));
		beginningOfMonth.set(Calendar.MINUTE, beginningOfMonth.getMinimum(Calendar.MINUTE));
		beginningOfMonth.set(Calendar.SECOND, beginningOfMonth.getMinimum(Calendar.SECOND));
		beginningOfMonth.set(Calendar.MILLISECOND, beginningOfMonth.getMinimum(Calendar.MILLISECOND));
		// adjust by lookahead month number
		beginningOfMonth.add(Calendar.MONTH, lookaheadMonths);

		// embargo expires within given number of months if it's no later than the end of that month
		return liftDate.toDate().before(beginningOfMonth.getTime());
	}

	private static boolean checkExpired(DCDate liftDate) {
		// check if it has expired
		Calendar beginningOfToday = Calendar.getInstance(Locale.getDefault());
		beginningOfToday.setTime(new Date());
		// reset to beginning of today
		beginningOfToday.set(Calendar.HOUR_OF_DAY, beginningOfToday.getMinimum(Calendar.HOUR_OF_DAY));
		beginningOfToday.set(Calendar.MINUTE, beginningOfToday.getMinimum(Calendar.MINUTE));
		beginningOfToday.set(Calendar.SECOND, beginningOfToday.getMinimum(Calendar.SECOND));
		beginningOfToday.set(Calendar.MILLISECOND, beginningOfToday.getMinimum(Calendar.MILLISECOND));

		// embargo is expired if embargo date is no later than beginning of today
		return !liftDate.toDate().after(beginningOfToday.getTime());
	}

	private String buildMessage() {
		if (items.isEmpty()) {
			return "No items are currently subject to a thesis embargo.";
		}
		StringBuilder builder = new StringBuilder();
		for (EmbargoedItemInfo item : items) {
			if (builder.length() > 0) {
				builder.append("\n\n");
			}

			builder.append("Lift date: \t");
			builder.append(item.liftDate.displayLocalDate(false, Locale.getDefault()));
			builder.append("\nTitle: \t");
			builder.append(item.title);
			builder.append("\nAuthor(s): \t");
			builder.append(item.authors);
			builder.append("\nLink: \t");
			builder.append(item.handle);
		}

		return builder.toString();
	}

	private class EmbargoedItemInfo {
		private final String title;
		private final DCDate liftDate;
		private final String authors;
		private final int id;
		private String handle;

		public EmbargoedItemInfo(Item item, DCDate liftDate) {
			this.title = item.getName();
			this.liftDate = liftDate;

			this.handle = item.getHandle();
			if (handle != null) {
				handle = HandleManager.getCanonicalForm(handle);
			} else {
				handle = "(workflow item)";
			}

			StringBuilder authors = new StringBuilder();
			DCValue[] authorMetadata = item.getMetadata("dc", "contributor", "author", Item.ANY);
			for (DCValue author : authorMetadata) {
				if (author.value != null && !"".equals(author.value)){
					if (authors.length() > 0) {
						authors.append("; ");
					}
					authors.append(author.value);
				}
			}
			this.authors = authors.toString();
			this.id = item.getID();
		}
	}

	private class EmbargoedItemInfoComparator implements Comparator<EmbargoedItemInfo> {
		@Override
		public int compare(EmbargoedItemInfo o1, EmbargoedItemInfo o2) {
			// first by embargo lift date, then by title
			try
			{
				if (!o1.liftDate.toDate().equals(o2.liftDate.toDate()))
				{
					return o1.liftDate.toDate().compareTo(o2.liftDate.toDate());
				}
			}
			catch (Exception ex)
			{
				log.warn("Exception caught while trying to sort embargoed items", ex);
			}

			if (o1.title != null && o2.title != null)
			{
				return o1.title.compareToIgnoreCase(o2.title);
			}
			// worst case, fall back on ordering by ID
			return new Integer(o1.id).compareTo(o2.id);
		}
	}
}
