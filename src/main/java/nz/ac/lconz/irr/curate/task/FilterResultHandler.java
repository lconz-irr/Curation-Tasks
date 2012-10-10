package nz.ac.lconz.irr.curate.task;

import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.log4j.Logger;

/**
 * @author Andrea Schweer schweer@waikato.ac.nz for LCoNZ
 * based on PrintResultHandler in http://commons.apache.org/exec/xref-test/org/apache/commons/exec/TutorialTest.html
 */
public class FilterResultHandler extends DefaultExecuteResultHandler {
	private ExecuteWatchdog watchdog;
	private Logger log = Logger.getLogger(FilterResultHandler.class);

	public FilterResultHandler(ExecuteWatchdog watchdog) {
		this.watchdog = watchdog;
	}

	@Override
	public void onProcessComplete(int exitValue) {
		super.onProcessComplete(exitValue);
		log.info("Completed filtering successfully");
	}

	@Override
	public void onProcessFailed(ExecuteException e) {
		super.onProcessFailed(e);
		if(watchdog != null && watchdog.killedProcess()) {
			log.warn("Conversion process timed out");
		} else {
			log.warn("Conversion process failed");
		}
	}
}
