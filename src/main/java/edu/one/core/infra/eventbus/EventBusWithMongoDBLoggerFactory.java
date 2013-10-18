package edu.one.core.infra.eventbus;

import com.wse.eventbus.EventBusWrapperFactory;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.EventBus;

public class EventBusWithMongoDBLoggerFactory implements EventBusWrapperFactory{

	@Override
	public EventBus getEventBus(Vertx vertx) {
		return new EventBusWithMongoDBLogger(vertx.eventBus());
	}

}