package nz.ac.lconz.irr.curate.task.format;

import nz.ac.lconz.irr.curate.task.FilterResultHandler;
import org.apache.commons.exec.*;
import org.apache.log4j.Logger;
import org.dspace.content.Bitstream;
import org.dspace.content.Item;
import org.dspace.core.Utils;

import java.io.*;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class VideoConversionHelper {
	private static final Logger log = Logger.getLogger(VideoConversionHelper.class);
	public static final int DEFAULT_TIMEOUT = 1000 * 60 * 10;

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
		File sourceTmp = File.createTempFile("VideoConversionSource" + sourceStream.hashCode(),".tmp");
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
