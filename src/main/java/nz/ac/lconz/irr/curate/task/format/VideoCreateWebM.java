package nz.ac.lconz.irr.curate.task.format;

import org.apache.commons.exec.*;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.Bitstream;
import org.dspace.content.Item;
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
public class VideoCreateWebM extends MediaFilter {
	private static final Logger log = Logger.getLogger(VideoCreateWebM.class);
	private static final int TIMEOUT = 3 * 60 * 1000; // 3 ms

	private static final VideoConversionHelper helper = new VideoConversionHelper();

	@Override
	protected boolean canFilter(Item item, Bitstream bitstream) {
		return helper.canFilter(item, bitstream);
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
	 * @throws AuthorizeException If the current user isn't authorised to perform this operation
	 * @throws IOException
	 * @throws SQLException
	 */
	@Override
	protected boolean filterBitstream(Item item, Bitstream bitstream) throws AuthorizeException, IOException, SQLException {
		String inFileName = helper.makeTempInFile(bitstream.retrieve());

		return createWebm(item, bitstream, inFileName);
	}

	// ffmpeg -i gentle-movement-original.mov -b 1500k -vcodec libvpx -acodec libvorbis -ab 160000 -f webm -g 30 gentle-movement-original.mov.webm
	private boolean createWebm(Item item, Bitstream bitstream, String inFileName) {
		final String outFileName = inFileName + ".mp4";

		Map<String, File> map = new HashMap();
		map.put("infile", new File(inFileName));
		map.put("outfile", new File(outFileName));

		// see http://commons.apache.org/exec/commandline.html
		CommandLine cmdLine = CommandLine.parse("/usr/bin/ffmpeg -i ${infile} -b 1500k -vcodec libvpx -acodec libvorbis -ab 160000 -f webm -g 30 -s 320x240 ${outfile}", map);

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
