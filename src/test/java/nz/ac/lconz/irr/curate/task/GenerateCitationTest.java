package nz.ac.lconz.irr.curate.task;

import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for GenerateCitation curation task.
 * @author Andrea Schweer schweer@waikato.ac.nz for the LCoNZ Institutional Research Repositories
 */
public class GenerateCitationTest {

	private static final Logger log = Logger.getLogger(GenerateCitationTest.class);
/*
	@Test public void testMakeCitation() {
		String testJSON = "{\n" +
				                  "\t\t\"id\": \"ITEM-1\",\n" +
				                  "\t\t\"author\": [\n" +
				                  "\t\t\t{\n" +
				                  "\t\t\t\t\"family\": \"Razlogova\",\n" +
				                  "\t\t\t\t\"given\": \"Elena\"\n" +
				                  "\t\t\t}\n" +
				                  "\t\t],\n" +
				                  "\t\t\"title\": \"Radio and Astonishment: The Emergence of Radio Sound, 1920-1926\",\n" +
				                  "\t\t\"type\": \"speech\",\n" +
				                  "\t\t\"event\": \"Society for Cinema Studies Annual Meeting\",\n" +
				                  "\t\t\"event-place\": \"Denver, CO\",\n" +
				                  "        \"note\":\"All styles in the CSL repository are supported by the new processor, including the popular Chicago styles by Elena.\",\n" +
				                  "\t\t\"issued\": {\n" +
				                  "\t\t\t\"date-parts\": [\n" +
				                  "\t\t\t\t[\n" +
				                  "\t\t\t\t\t2002,\n" +
				                  "\t\t\t\t\t5\n" +
				                  "\t\t\t\t]\n" +
				                  "\t\t\t]\n" +
				                  "\t\t}\n" +
				                  "}";
		String citation = null;
		try {
			citation = GenerateCitation.makeCitation(testJSON, "apa6", "en_GB").trim();
		} catch (GenerateCitation.CitationGenerationException e) {
			fail("Exception thrown: " + e.getMessage());
		}
		log.info("citation result: " + citation);
		assertEquals("incorrect citation generated", "Razlogova, E. (2002, May). Radio and Astonishment: The Emergence of Radio Sound, 1920-1926. Presented at the Society for Cinema Studies Annual Meeting.", citation);
	}*/
}
