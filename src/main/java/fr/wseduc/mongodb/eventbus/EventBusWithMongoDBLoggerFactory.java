package fr.wseduc.mongodb.eventbus;

import fr.wseduc.vertx.eventbus.EventBusWrapperFactory;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.EventBus;

public class EventBusWithMongoDBLoggerFactory implements EventBusWrapperFactory {

	@Override
	public EventBus getEventBus(Vertx vertx) {
		return new EventBusWithMongoDBLogger(vertx.eventBus());
	}

}