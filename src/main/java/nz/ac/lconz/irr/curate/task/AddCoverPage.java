package nz.ac.lconz.irr.curate.task;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Bundle;
import org.dspace.content.DCValue;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.ConfigurationManager;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Mutative;
import org.dspace.handle.HandleManager;

import com.itextpdf.text.DocumentException;
import com.itextpdf.text.pdf.AcroFields;
import com.itextpdf.text.pdf.PdfCopyFields;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfStamper;

/** 
 * DSpace curation task to add a cover page to each eligible bitstream of a DSpace item.
 * @author Andrea Schweer <schweer@waikato.ac.nz>
 *
 */
@Mutative
public class AddCoverPage extends AbstractCurationTask {

	private static final String PLUGIN_PREFIX = "addcover";
	private String coverFilename;
	private String preservationBundleName;

	/**
	 * Performs the actual work: adds a cover page to all bitstreams that are in applicable formats (@see {@link #acceptedFormat(Bitstream)).}
	 * @param dso the DSpace object to work on; this should be an item.
	 * @return CURATE_SKIP if the dso is not an item; 
	 *   CURATE_SUCCESS if at least one bitstream was modified;
	 *   CURATE_FAIL if no bitstreams were modified.
	 * @throws IOException if an error is encountered during processing.
	 */
	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (!(dso instanceof Item)) {
			return Curator.CURATE_SKIP;
		}
		Item item = (Item) dso;
		
		int processedBitstreams = 0;
		boolean hasError = false;
		
		try {
			Bundle[] bundles = item.getBundles("ORIGINAL");
			for (Bundle bundle : bundles) {
				Bitstream[] bitstreams = bundle.getBitstreams();
				for (Bitstream bitstream : bitstreams) {
					if (!acceptedFormat(bitstream)) {
						report(bitstream.getName() + " skipped: not an accepted format");
						continue; // go to next bitstream
					}

					try {
						File tempFile = File.createTempFile("addcover-" + bitstream.getName(), ".pdf");
						tempFile.deleteOnExit();
						FileOutputStream output = new FileOutputStream(tempFile);
						addCoverPage(bitstream, item, output);

						Bundle preservationBundle = null;
						Bundle[] preservationBundleCandidates = item.getBundles(preservationBundleName);
						if (preservationBundleCandidates != null && preservationBundleCandidates.length > 0) {
							preservationBundle = preservationBundleCandidates[0];
						} else {
							preservationBundle = item.createBundle(preservationBundleName);
						}
						preservationBundle.addBitstream(bitstream);
						bundle.removeBitstream(bitstream);
						
						Bitstream coverFileBitstream = bundle.createBitstream(new FileInputStream(tempFile));
						
						coverFileBitstream.setSource(this.getClass().getName());
						coverFileBitstream.setName(bitstream.getName());
						coverFileBitstream.setFormat(bitstream.getFormat());
						coverFileBitstream.update();
						
						bundle.update();
						preservationBundle.update();
					} catch (DocumentException e) {
						String message = bitstream.getName() + ": failed to add cover page to file " + bitstream.getName();
						report(message);
						setResult(message);
						System.err.println("problem: " + e.getMessage());
						e.printStackTrace(System.err);
						hasError = true;
					}
					
					report(bitstream.getName() + ": added cover page; original moved to preservation bundle");
					processedBitstreams++;
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

	
	
	@Override
	public void init(Curator curator, String taskId) throws IOException {
        super.init(curator, taskId);
        coverFilename = ConfigurationManager.getProperty(PLUGIN_PREFIX, "cover.filename");
        if (coverFilename == null) {
        	throw new IOException("Need filename for cover page in config file");
        }
        preservationBundleName = ConfigurationManager.getProperty(PLUGIN_PREFIX, "moveto.bundle");
	}



	private void addCoverPage(Bitstream bitstream, Item item, OutputStream out) throws IOException, SQLException, AuthorizeException, DocumentException {
		InputStream content = bitstream.retrieve();
		
		PdfReader original = new PdfReader(content);
		ByteArrayOutputStream coverBuffer = new ByteArrayOutputStream();
		PdfStamper cover = new PdfStamper(new PdfReader(coverFilename), coverBuffer);
		AcroFields form = cover.getAcroFields();
		if (form != null)
		{
			completeForm(form, item);
			cover.setFormFlattening(true);
		}
		cover.close();
		
		PdfCopyFields copy = new PdfCopyFields(out);
		PdfReader completedCover = new PdfReader(coverBuffer.toByteArray());
		copy.addDocument(completedCover);
		copy.addDocument(original);
		
		copy.close();
	}



	@SuppressWarnings("deprecation")
	private void completeForm(AcroFields form, Item item) throws IOException,
			DocumentException {
		for (String fieldName : form.getFields().keySet()) {
			String value = "";
			if ("handle".equals(fieldName)) {
				value = HandleManager.getCanonicalForm(item.getHandle());
			} else if ("dspace_name".equals(fieldName)) {
				value = ConfigurationManager.getProperty("dspace.name");
			} else if ("dspace_url".equals(fieldName)) {
				value = ConfigurationManager.getProperty("dspace.url");				
			} else {
				String dcField = fieldName.replace("_", ".");
				DCValue[] dcValues = item.getMetadata(dcField);
				if (dcValues != null && dcValues.length > 0) {
					StringBuilder valueBuilder = new StringBuilder();
					for (DCValue dcValue : dcValues) {
						if (valueBuilder.length() > 0) {
							valueBuilder.append("; "); 
						}
						valueBuilder.append(dcValue.value);
					}
					value = valueBuilder.toString();
				}
			}
			if (value == null) {
				value = "";
			}
			form.setField(fieldName, value);
		}
	}

	private boolean acceptedFormat(Bitstream bitstream) {
		return "application/pdf".equals(bitstream.getFormat().getMIMEType());
	}

}
