package nz.ac.lconz.irr.curate.task;

import nz.ac.lconz.irr.crosswalk.citeproc.CiteprocCrosswalk;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.DSpaceObject;
import org.dspace.content.Item;
import org.dspace.content.Metadatum;
import org.dspace.content.crosswalk.CrosswalkException;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;
import org.dspace.curate.Mutative;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.*;
import java.sql.SQLException;

/**
 * @author Andrea Schweer for the LCoNZ Institutional Research Repositories
 *
 * Curation task to automatically generate a citation from the item's metadata.
 */
@Mutative
public class GenerateCitation extends AbstractCurationTask {
	private static final Logger log = Logger.getLogger(GenerateCitation.class);

	@Override
	public int perform(DSpaceObject dSpaceObject) throws IOException {
		if (dSpaceObject == null || dSpaceObject.getType() != Constants.ITEM) {
			return Curator.CURATE_SKIP;
		}

		String field = taskProperty("field");
		if (field == null || "".equals(field)) {
			field = "dc.identifier.citation";
			log.info(taskId + ": no field configured, using default (" + field + ")");
		}
		String[] components = field.split("\\.");
		String schema, element, qualifier;
		try {
			schema = components[0];
			element = components[1];
			qualifier = components.length > 2 ? components[2] : null;
		} catch (RuntimeException e) {
			String message = taskId + ": invalid setting for field name (" + field + "), aborting";
			log.fatal(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}

		Item item = (Item) dSpaceObject;

		Metadatum[] existingCitation = item.getMetadata(schema, element, qualifier, Item.ANY);
		boolean hasCitation = (existingCitation != null && existingCitation.length > 0 && existingCitation[0].value != null && !"".equals(existingCitation[0].value));

		boolean overrideExisting = taskBooleanProperty("force", false);

		if (hasCitation && !overrideExisting) {
			String message = "Item already has citation, skipping; item_id=" + item.getID();
			log.info(message);
			report(message);
			setResult(message);
			return Curator.CURATE_SKIP;
		}

		Context context = null;
		String itemJSON;
		try {
			context = Curator.curationContext();
			itemJSON = itemToCiteprocJSON(context, item);
		} catch (SQLException e) {
			String message = taskId + "Problem generating citation";
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} catch (CrosswalkException e) {
			String message = taskId + "Problem generating citation";
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} catch (AuthorizeException e) {
			String message = taskId + "Problem generating citation";
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}

		if (itemJSON == null || "".equals(itemJSON)) {
			String message = taskId + "Problem extracting metadata from item";
			log.error(message);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}

		String citationStyle = taskProperty("style");
		String citationLocale = taskProperty("locale");
		String citation;
		try {
			citation = makeCitation(itemJSON, citationStyle, citationLocale.replaceAll("_", "-"));
		} catch (CitationGenerationException e) {
			String message = taskId + "Problem generating citation";
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}

		if (citation == null || "".equals(citation)) {
			String message = taskId + ": empty citation for item id=" + item.getID();
			log.info(message);
			report(message);
			setResult(message);
			return Curator.CURATE_FAIL;
		}

		item.clearMetadata(schema, element, qualifier, Item.ANY);
		item.addMetadata(schema, element, qualifier, "en", citation);

		try {
			item.update();
			setResult("Added citation " + citation);
			report("Successfully added citation to item id=" + item.getID());
		} catch (SQLException e) {
			String message = taskId + "Problem adding citation to item";
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		} catch (AuthorizeException e) {
			String message = taskId + "Problem adding citation to item";
			log.error(message, e);
			report(message);
			setResult(message);
			return Curator.CURATE_ERROR;
		}

		return Curator.CURATE_SUCCESS;
	}

	private String itemToCiteprocJSON(Context context, Item item) throws CrosswalkException, AuthorizeException, IOException, SQLException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		new CiteprocCrosswalk().disseminate(context, item, baos);
		try {
			return baos.toString("UTF-8");
		} catch (UnsupportedEncodingException e) {
			log.error("Problem converting item to json string", e);
			return "";
		}
	}

	static String makeCitation(String itemJSON, String citationStyle, String locale) throws CitationGenerationException {
		//String[] files = { "xmle4x.js", "citeproc.js", "loadcites.js", "make-citation.js" };
		String[] files = { "xmle4x.js", "citeproc.js", "make-citation.js" };
		String result = null;

		ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");
		engine.put("log", log);
		try {
			String styleDef = loadStyle(citationStyle);
			engine.put("style", styleDef);

			String localeDef = loadLocale(locale);
			engine.put("locale", localeDef);

			engine.eval("var data = " + itemJSON + ";");

			for (String file : files) {
				engine.eval(new FileReader(new File(ConfigurationManager.getProperty("dspace.dir") + "/config/modules/citation/js/" + file)));
			}
			Object resultObj = engine.eval("makeCitation();");
			result = resultObj.toString();
		} catch (ScriptException e) {
			log.fatal("Cannot make citation", e);
		} catch (FileNotFoundException e) {
			log.fatal("Cannot make citation", e);
		}
		return result;
	}

	private static String loadLocale(String locale) throws CitationGenerationException {
		locale = locale.replaceAll("_", "-");
		String filename = ConfigurationManager.getProperty("dspace.dir") + "/config/modules/citation/locale/locale-" + locale + ".xml";
		File file = new File(filename);
		if (!file.exists()) {
			log.warn("No locale file found for requested locale " + locale + "; falling back to default");
			file = new File(ConfigurationManager.getProperty("dspace.dir") + "/config/modules/citation/locale/locale-en-GB.xml");
		}
		try {
			return FileUtils.readFileToString(file);
		} catch (IOException e) {
			throw new CitationGenerationException("No usable locale found", e);
		}
	}

	private static String loadStyle(String citationStyle) throws CitationGenerationException {
		String filename = ConfigurationManager.getProperty("dspace.dir") + "/config/modules/citation/csl/" + citationStyle + ".xml";
		File file = new File(filename);
		if (!file.exists()) {
			log.warn("No locale file found for requested style " + citationStyle + "; falling back to default");
			file = new File(ConfigurationManager.getProperty("dspace.dir") + "/config/modules/citation/csl/apa6.xml");
		}
		try {
			return FileUtils.readFileToString(file);
		} catch (IOException e) {
			throw new CitationGenerationException("No usable style found", e);
		}
	}

	static class CitationGenerationException extends Throwable {
		public CitationGenerationException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
