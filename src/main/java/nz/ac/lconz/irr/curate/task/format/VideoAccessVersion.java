package nz.ac.lconz.irr.curate.task.format;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Item;
import org.dspace.ctask.mediafilter.MediaFilter;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class VideoAccessVersion extends MediaFilter {
	private static final Logger log = Logger.getLogger(VideoAccessVersion.class);

	private static final VideoConversionHelper helper = new VideoConversionHelper();

	@Override
	protected boolean canFilter(Item item, Bitstream bitstream) {
		return bitstream.getFormat().getMIMEType().startsWith("video/");
	}

	@Override
	protected boolean filterBitstream(Item item, Bitstream bitstream) throws AuthorizeException, IOException, SQLException {
		String inFileName = helper.makeTempInFile(bitstream.retrieve());

		return createAccessCopy(item, bitstream, inFileName);
	}

	private boolean createAccessCopy(Item item, Bitstream bitstream, String inFileName) {
		String command = taskProperty("command");
		command = command.replaceAll("%", Matcher.quoteReplacement("$"));

		String suffix = taskProperty("target.suffix");

		final String outFileName = inFileName + suffix;

		Map<String, File> map = new HashMap();
		map.put("infile", new File(inFileName));
		map.put("outfile", new File(outFileName));

		// see http://commons.apache.org/exec/commandline.html
		CommandLine cmdLine = CommandLine.parse(command, map);

		try {
			int status = helper.run(cmdLine, VideoConversionHelper.DEFAULT_TIMEOUT);
			if (status == 0) {
				FileInputStream fis = new FileInputStream(outFileName);
				byte[] resultData = IOUtils.toByteArray(fis);
				IOUtils.closeQuietly(fis);
				new File(outFileName).deleteOnExit();

				createDerivative(item, bitstream, new ByteArrayInputStream(resultData));
				return true;
			}
		} catch (Exception e) {
			log.error("Cannot create access version (" + taskProperty("target.format") + ") of bitstream " + bitstream.getID());
		}
		return false;
	}
}
