package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Mutative;
import org.dspace.license.CreativeCommons;
import org.jaxen.JaxenException;
import org.jaxen.jdom.JDOMXPath;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: schweer
 * Date: 27/06/12
 * Time: 5:04 PM
 * To change this template use File | Settings | File Templates.
 */
@Mutative
public class AddCCMetadata extends AbstractCurationTask {

	private static final Logger log = Logger.getLogger(AddCCMetadata.class);

	@Override
	public int perform(DSpaceObject dso) throws IOException {
		if (dso == null || dso.getType() != Constants.ITEM) {
			return Curator.CURATE_SKIP;
		}

		Item item = (Item) dso;

		Context context = null;
		try {
			context = Curator.curationContext();
			if (!CreativeCommons.hasLicense(context, item)) {
				String message = "Item id=" + item.getID() + " does not have a CC license";
				report(message);
				setResult(message);

				return Curator.CURATE_SKIP;
			}

			boolean uriChanged, nameChanged;

			uriChanged = addUriField(item);
			nameChanged = addNameField(item);

			if (uriChanged || nameChanged) {
				item.update();

				context.commit();

				String message = "Set license uri/name for item id=" + item.getID();
				report(message);
				setResult(message);
				return Curator.CURATE_SUCCESS;
			} else {
				String message = "Item id=" + item.getID() + " already has correct CC metadata";
				report(message);
				setResult(message);
				return Curator.CURATE_FAIL;
			}
		} catch (Exception e) {
			String message = "Problem adding CC metadata to item, item id=" + item.getID() + ": " + e.getMessage();
			report(message);
			setResult(message);
			log.error(message, e);

			return Curator.CURATE_ERROR;
		}
	}

	private boolean addNameField(Item item) throws AuthorizeException, IOException, SQLException, JaxenException, JDOMException {
		CreativeCommons.MdField nameField = CreativeCommons.getCCField("name");
		String nameValue = nameField.ccItemValue(item);

		if (nameValue == null || "".equals(nameValue)) {
			String licenseName = getLicenseName(item);
			if (licenseName != null && !"".equals(licenseName)) {
				nameField.addItemValue(item, licenseName);
			}
			return true;
		}
		return false;
	}

	private boolean addUriField(Item item) throws SQLException, IOException, AuthorizeException {
		CreativeCommons.MdField uriField = CreativeCommons.getCCField("uri");
		String uriValue = uriField.ccItemValue(item);

		if (uriValue == null || "".equals(uriValue)) {
			String licenseURL = CreativeCommons.getLicenseURL(item);
			if (licenseURL != null && !"".equals(licenseURL)) {
				uriField.addItemValue(item, licenseURL);
			}
			return true;
		}
		return false;
	}

	private String getLicenseName(Item item) throws AuthorizeException, IOException, SQLException, JaxenException, JDOMException {
		Bitstream rdfBitstream = CreativeCommons.getLicenseRdfBitstream(item);

		SAXBuilder parser = new SAXBuilder();
		Document doc = parser.build(rdfBitstream.retrieve());

		JDOMXPath licenseNameXPath = new JDOMXPath("//rdf:RDF/cc:License//dc:title[@xml:lang='en']");

		licenseNameXPath.addNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		licenseNameXPath.addNamespace("cc", "http://creativecommons.org/ns#");
		licenseNameXPath.addNamespace("dc", "http://purl.org/dc/elements/1.1/");
		licenseNameXPath.addNamespace("xml", "http://www.w3.org/XML/1998/namespace");

		Element element = (Element) licenseNameXPath.selectSingleNode(doc);
		if (element != null) {
			return element.getText();
		} else {
			return null;
		}
	}
}

