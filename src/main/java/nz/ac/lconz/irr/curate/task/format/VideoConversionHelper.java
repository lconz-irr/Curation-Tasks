package nz.ac.lconz.irr.curate.task.format;

import nz.ac.lconz.irr.curate.task.FilterResultHandler;
import org.apache.commons.exec.*;
import org.apache.log4j.Logger;
import org.dspace.content.Bitstream;
import org.dspace.content.Item;
import org.dspace.core.Utils;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: schweer
 * Date: 3/09/12
 * Time: 3:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class VideoConversionHelper {
	private static final Logger log = Logger.getLogger(VideoConversionHelper.class);

	/**
	 * Check whether this filter applies to a given bitstream.
	 * This particular task applies to all video files (ie bitstreams whose format mimetype starts with "video/").
	 *
	 *
	 * @param item The item whose bitstream is to be filtered
	 * @param bitstream The bitstream to be filtered
	 * @return True when this filter applies to this particular bitstream, false otherwise
	 */
	protected boolean canFilter(Item item, Bitstream bitstream) {
		return bitstream.getFormat().getMIMEType().startsWith("video/");
	}

	protected int run(CommandLine cmdLine, int timeout) throws Exception {
		Executor executor = new DefaultExecutor();
		ExecuteWatchdog watchdog = new ExecuteWatchdog(timeout);
		executor.setWatchdog(watchdog);
		executor.setWorkingDirectory(new File(System.getProperty("java.io.tmpdir")));

		DefaultExecuteResultHandler resultHandler;
		try {
			resultHandler = new FilterResultHandler(watchdog);
			executor.execute(cmdLine, resultHandler);
		} catch (Exception e) {
			log.error("Problem running command line " + cmdLine.toString(), e);
			throw e;
		}
		resultHandler.waitFor();
		return resultHandler.getExitValue();
	}

	protected String makeTempInFile(InputStream sourceStream) throws IOException {
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
}
