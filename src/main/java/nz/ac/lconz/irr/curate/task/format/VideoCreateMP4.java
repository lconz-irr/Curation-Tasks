package nz.ac.lconz.irr.curate.task.format;

import nz.ac.lconz.irr.curate.task.FilterResultHandler;
import org.apache.commons.exec.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Item;
import org.dspace.core.Utils;
import org.dspace.ctask.mediafilter.MediaFilter;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Curation task to generate access copies of video files. Uses the media filter curation task framework from MIT (https://github.com/richardrodgers/ctask).
 *
 * @author Andrea Schweer schweer@waikato.ac.nz for LCoNZ
 *
 */
public class VideoCreateMP4 extends MediaFilter {
	private static final Logger log = Logger.getLogger(VideoCreateMP4.class);
	private static final int TIMEOUT = 3 * 60 * 1000; // 3 ms

	private static final VideoConversionHelper helper = new VideoConversionHelper();


	/**
	 * Check whether this filter applies to a given bitstream.
	 * This particular task applies to all video files (ie bitstreams whose format mimetype starts with "video/").
	 *
	 *
	 * @param item The item whose bitstream is to be filtered
	 * @param bitstream The bitstream to be filtered
	 * @return True when this filter applies to this particular bitstream, false otherwise
	 */
	@Override
	protected boolean canFilter(Item item, Bitstream bitstream) {
		return helper.canFilter(null, bitstream);
	}

	/**
	 * Actually filter the given bitstream.
	 * This particular task will attempt to invoke ffmpeg to create 2 derivative versions of the bitstream: WebM and MP4 (h264).
	 * Both derivative versions will be at most 320x240 pixels.
	 *
	 * The task will fail unless both versions can be created.
	 *
	 * @param item The item whose bitstream is to be filtered
	 * @param bitstream The bitstream to be filtered
	 * @return True if filtering was successful, false otherwise.
	 * @throws org.dspace.authorize.AuthorizeException If the current user isn't authorised to perform this operation
	 * @throws java.io.IOException
	 * @throws java.sql.SQLException
	 */
	@Override
	protected boolean filterBitstream(Item item, Bitstream bitstream) throws AuthorizeException, IOException, SQLException {
		String inFileName = helper.makeTempInFile(bitstream.retrieve());

		return createMp4(item, bitstream, inFileName);
	}

	// /usr/local/bin/ffmpeg -i gentle-movement-original.mov -vcodec libx264 -acodec libfaac -s 320x240 gentle-movement-original.mov.mp4
	private boolean createMp4(Item item, Bitstream bitstream, String inFileName) throws IOException, AuthorizeException, SQLException {
		final String outFileName = inFileName + ".mp4";

		Map<String, File> map = new HashMap();
		map.put("infile", new File(inFileName));
		map.put("outfile", new File(outFileName));

		// see http://commons.apache.org/exec/commandline.html
		CommandLine cmdLine = CommandLine.parse("/usr/bin/ffmpeg -i ${infile} -vcodec libx264 -acodec libfaac -s 320x240 ${outfile}", map);

		try {
			int status = helper.run(cmdLine, TIMEOUT);
			if (status == 0) {
				FileInputStream fis = new FileInputStream(outFileName);
				byte[] resultData = IOUtils.toByteArray(fis);
				IOUtils.closeQuietly(fis);
				new File(outFileName).deleteOnExit();

				createDerivative(item, bitstream, new ByteArrayInputStream(resultData));
				return true;
			}
		} catch (Exception e) {
			log.error("Cannot create MP4 version of bitstream " + bitstream.getID());
		}
		return false;
	}

}
