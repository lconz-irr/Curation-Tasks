package nz.ac.lconz.irr.curate.task;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.DCDate;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;

import com.itextpdf.text.pdf.PdfReader;

public class ExtractPdfMetadata extends AbstractCurationTask {

	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (!(dso instanceof Item)) {
			return Curator.CURATE_SKIP;
		}
		Item item = (Item) dso;

		boolean hasError = false;
		int processedBitstreams = 0;

		try {
			Bundle[] bundles = item.getBundles("ORIGINAL");
			for (Bundle bundle : bundles) {
				Bitstream[] bitstreams = bundle.getBitstreams();
				for (Bitstream bitstream : bitstreams) {
					if (!acceptedFormat(bitstream)) {
						report(bitstream.getName()
								+ " skipped: not an accepted format");
						continue; // go to next bitstream
					}
					try {
						boolean success = extractMetadata(bitstream, item);
						if (success) {
							item.update();
							processedBitstreams++;
						}
					} catch (SQLException e) {
						hasError = true;
					}
				}
			}
		} catch (SQLException e) {
			System.err.println("problem: " + e.getMessage());
			e.printStackTrace(System.err);
			throw new IOException(e);
		} catch (AuthorizeException e) {
			System.err.println("problem: " + e.getMessage());
			e.printStackTrace(System.err);
			throw new IOException(e);
		}

		if (hasError) {
			setResult("There were problems with one or more files.");
			return Curator.CURATE_FAIL;
		}

		if (processedBitstreams > 0) {
			setResult(processedBitstreams + " file(s) processed successfully");
			return Curator.CURATE_SUCCESS;
		} else {
			return Curator.CURATE_SKIP;
		}
	}

	private boolean extractMetadata(Bitstream bitstream, Item item) throws AuthorizeException, SQLException, IOException {
		boolean madeChanges = false;
		InputStream contentStream = bitstream.retrieve();
		PdfReader reader = new PdfReader(contentStream);
		Map<String,String> info = reader.getInfo();
		if (info.containsKey("Title")) {
			String title = info.get("Title");
			item.addMetadata("dc", "title", null, null, title);
			report(item.getHandle() + ": added title (" + title + ") from " + bitstream.getName());
			madeChanges = true;
		}
		if (info.containsKey("Author")) {
			String author = info.get("Author");
			item.addMetadata("dc", "contributor", "author", null, author);
			report(item.getHandle() + ": added author (" + author + ") from " + bitstream.getName());
			madeChanges = true;
		}
		if (info.containsKey("CreationDate")) {
			String dateString = info.get("CreationDate");
			try {
				Date creationDate = DATE_FORMAT.parse(dateString.substring(2, 8));
				DCDate mdDate = new DCDate(creationDate);
				item.addMetadata("dc", "date", "issued", null, mdDate.toString());
				report(item.getHandle() + ": added creation date (" + mdDate.toString() + ") from " + bitstream.getName());
				madeChanges = true;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return madeChanges;
	}

	private boolean acceptedFormat(Bitstream bitstream) {
		return "application/pdf".equals(bitstream.getFormat().getMIMEType());
	}
}
