package nz.ac.lconz.irr.curate.task;

import nz.ac.lconz.irr.utils.AuthorizeUtils;
import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;import org.dspace.content.BitstreamFormat;import org.dspace.content.Bundle;
import org.dspace.content.Item;
import org.dspace.core.ConfigurationManager;
import org.dspace.core.Utils;
import org.dspace.curate.Curator;

import java.io.BufferedInputStream;import java.io.File;import java.io.FileInputStream;import java.io.FileOutputStream;import java.io.IOException;import java.io.InputStream;import java.io.OutputStream;import java.lang.IllegalArgumentException;import java.lang.IllegalStateException;import java.lang.InterruptedException;import java.lang.Override;import java.lang.Process;import java.lang.String;import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for LCoNZ
 */
public class ImageMagickThumbnailer extends org.dspace.ctask.mediafilter.MediaFilter {

	private static Logger log = Logger.getLogger(ImageMagickThumbnailer.class);

	// optional task configuration properties
	private Properties taskProps = null;

	// from thumbnail.thumbWidth in config
	private int thumbWidth = 200;
	private String convertPath;

	private static final String CONVERT_COMMAND = "@COMMAND@ -thumbnail @WIDTH@x @INFILE@[0] @OUTFILE@";
	private static final List<String> EXTENSIONS = Arrays.asList(new String[] {".pdf", ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tif", ".tiff"});

	@Override
	public void init(Curator curator, String taskId) throws IOException {
		super.init(curator, taskId);
		log.debug("Loading settings for ImageMagick Thumbnailer");
		convertPath = taskProperty("imagemagick.path.convert");
		thumbWidth = taskIntProperty("thumbnail.maxwidth", thumbWidth);
	}

	@Override
	protected boolean canFilter(Item item, Bitstream bitstream) {
		if (!AuthorizeUtils.anonymousCanRead(item)) {
			return false;
		}
		if (!AuthorizeUtils.anonymousCanRead(bitstream)) {
			return false;
		}
		try {
			for (Bundle bundle : bitstream.getBundles()) {
				if (!AuthorizeUtils.anonymousCanRead(bundle)) {
					return false;
				}
			}
		} catch (SQLException e) {
			log.error("Cannot determine whether bitstream can be filtered: " + e.getMessage(), e);
			return false;
		}

		BitstreamFormat bsf = bitstream.getFormat();
		// look through the file suffixes. TODO make this configurable?
		for (String sfx: bsf.getExtensions()) {
			if (EXTENSIONS.contains(sfx)) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected boolean filterBitstream(Item item, Bitstream bitstream) throws AuthorizeException, IOException, SQLException {
		if (convertPath == null) {
			log.error("Cannot read configuration for ImageMagick Thumbnailer");
			throw new IllegalStateException("Imagemagick path not set");
		}

		String inFileName = makeTempInFile(bitstream.retrieve());
		String outFileNAme = inFileName + ".png";

		int status = executeConvertCommand(inFileName, outFileNAme);

		if (status != 0) {
			return false;
		}

		return createDerivative(item, bitstream, new BufferedInputStream(new FileInputStream(outFileNAme)));
	}

	private int executeConvertCommand(String inFileName, String outFileNAme) throws IOException {
		int status;
		String commandLine = CONVERT_COMMAND.replaceFirst("@COMMAND@", convertPath);
		commandLine = commandLine.replaceFirst("@WIDTH@", String.valueOf(thumbWidth));
		commandLine = commandLine.replaceFirst("@INFILE@", inFileName);
		commandLine = commandLine.replaceFirst("@OUTFILE@", outFileNAme);


		try {
			log.debug("About to run " + commandLine);
			Process convertProc = java.lang.Runtime.getRuntime().exec(commandLine);
			status = convertProc.waitFor();
		} catch (InterruptedException ie) {
			log.error("Failed to create thumbnail: ", ie);
			throw new IllegalArgumentException("Failed to create thumbnail: ", ie);
		}
		return status;
	}


	private String makeTempInFile(InputStream sourceStream) throws IOException {
		File sourceTmp = File.createTempFile("IMthumbSource" + sourceStream.hashCode(),".tmp");
		sourceTmp.deleteOnExit();
		try
		{
			OutputStream sto = new FileOutputStream(sourceTmp);
			Utils.copy(sourceStream, sto);
			sto.close();
		}
		finally
		{
			sourceStream.close();
		}

		if (!sourceTmp.canRead()) {
			log.error("Cannot read temporary file for input: " + sourceTmp.getCanonicalPath());
		}

		return sourceTmp.getCanonicalPath();
	}

	// TODO these really shouldn't be here -- hack to accommodate DSpace 1.7.2


	/**
	 * Returns task configuration property value for passed name, else
	 * <code>null</code> if no properties defined or no value for passed key.
	 *
	 * @param name
	 *        the property name
	 * @return value
	 *        the property value, or null
	 *
	 */
	protected String taskProperty(String name)
	{
		if (taskProps == null)
		{
			// load properties
			taskProps = new Properties();
			StringBuilder modName = new StringBuilder();
			for (String segment : taskId.split("\\."))
			{
				// load property segments if present
				modName.append(segment);
				Properties modProps = ConfigurationManager.getProperties(modName.toString());
				if (modProps != null)
				{
					taskProps.putAll(modProps);
				}
				modName.append(".");
			}
			// warn if *no* properties found
			if (taskProps.size() == 0)
			{
				log.warn("Warning: No configuration properties found for task: " + taskId);
			}
		}
		return taskProps.getProperty(name);
	}

	/**
	 * Returns task configuration integer property value for passed name, else
	 * passed default value if no properties defined or no value for passed key.
	 *
	 * @param name
	 *        the property name
	 * @param defaultValue value
	 *        the default value
	 * @return value
	 *        the property value, or default value
	 *
	 */
	protected int taskIntProperty(String name, int defaultValue)
	{
		int intVal = defaultValue;
		String strVal = taskProperty(name);
		if (strVal != null)
		{
			try
			{
				intVal = Integer.parseInt(strVal.trim());
			}
			catch(NumberFormatException nfE)
			{
				log.warn("Warning: Number format error in module: " + taskId + " property: " + name);
			}
		}
		return intVal;
	}

}
