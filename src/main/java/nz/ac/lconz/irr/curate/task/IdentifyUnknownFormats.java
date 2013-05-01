package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.dspace.authorize.AuthorizeException;
import org.dspace.content.*;
import org.dspace.core.Constants;
import org.dspace.curate.AbstractCurationTask;
import org.dspace.curate.Curator;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: dspace
 * Date: 30/04/13
 * Time: 11:58 AM
 * To change this template use File | Settings | File Templates.
 */
public class IdentifyUnknownFormats extends AbstractCurationTask {

    private static Logger log = Logger.getLogger(IdentifyUnknownFormats.class);

    private List<String> includeBundles;

    @Override
    public void init(Curator curator, String taskId) throws IOException {
        super.init(curator, taskId);
        String[] bundleArray = null;
        String bundleProperty = super.taskProperty("bundles.include");
        if (bundleProperty != null && !bundleProperty.equals("")) {
            bundleArray = bundleProperty.split(",\\s*");
        }
        if (bundleArray == null || bundleArray.length == 0) {
            bundleArray = new String[]{"ORIGINAL", "CONTENT"};
        }
        includeBundles = Arrays.asList(bundleArray);
    }

    @Override
    public int perform(DSpaceObject dSpaceObject) throws IOException {
        String message = "";
        if (dSpaceObject.getType() != Constants.ITEM) {
            return Curator.CURATE_SKIP;
        }

        Item item = (Item) dSpaceObject;
        try {
            Bundle[] bundles = item.getBundles();
            for (Bundle bundle : bundles) {
                if (includeBundles.contains(bundle.getName())) {
                    Bitstream[] bitstreams = bundle.getBitstreams();
                    for (Bitstream bitstream : bitstreams) {
                        BitstreamFormat format = bitstream.getFormat();
                        if (format.getSupportLevel() == BitstreamFormat.UNKNOWN) {
                            String extension = detectExtension(bitstream);
                            message = "Item handle: " + item.getHandle() + "\tUnknown format for bitstream: " + bitstream.getName() + extension;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            message = "Problem accessing bitstream for item id=" + item.getID() + ": " + e.getMessage();
            log.error(message, e);
            report(message);
            setResult(message);
            return Curator.CURATE_ERROR;
        }
        if (!message.equals("")) {
            report(message);
            setResult(message);
            log.info(message);
        }
        return Curator.CURATE_SUCCESS;
    }

    private String detectExtension(Bitstream bitstream) {
        Tika detector = new Tika();
        String extension = "";
        InputStream is;
        try {
            is = bitstream.retrieve();
            extension = detector.detect(is, bitstream.getName());
            is.close();
        } catch (Exception e) {
            log.warn("Cannot suggest MIME type for bitstream " + bitstream.getName() + " with id: " + bitstream.getID(), e);
            return "\tFormat suggestion failed";
        }
        return "\tSuggested format: " + extension;
    }
}
