package nz.ac.lconz.irr.curate;

import org.dspace.content.Item;
import org.dspace.core.Constants;
import org.dspace.core.Context;
import org.dspace.curate.Curator;
import org.dspace.event.Consumer;
import org.dspace.event.Event;

public class CurateOnInstallation implements Consumer {

	@Override
	public void consume(Context ctx, Event event) throws Exception {
		if (event.getSubjectType() != Constants.ITEM) {
			System.out.println("skipping non-item of type " + event.getObjectTypeAsString());
			return; // not an item -> ignore
		}
		Item item = (Item) event.getSubject(ctx);
		Curator curator = new Curator();
		// TODO make configurable
		// TODO queue vs execute immediately
		curator.addTask("addcover").queue(ctx, item.getHandle(), "continuously");
		System.out.println("curator has addcover task? " + curator.hasTask("addcover"));
	}

	@Override
	public void end(Context ctx) throws Exception {
	}

	@Override
	public void finish(Context ctx) throws Exception {
	}

	@Override
	public void initialize() throws Exception {
	}

}
