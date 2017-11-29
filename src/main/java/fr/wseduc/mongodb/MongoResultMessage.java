package fr.wseduc.mongodb;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

public class MongoResultMessage implements Message<JsonObject> {

	private final JsonObject body;

	public MongoResultMessage() {
		this(null);
	}

	public MongoResultMessage(JsonObject j) {
		if (j == null) {
			body = new JsonObject().put("status", "ok");
		} else {
			body = j;
			if (!j.containsKey("status")) {
				j.put("status", "ok");
			}
		}
	}

	public MongoResultMessage put(String attr, Object o) {
		body.put(attr, o);
		return this;
	}

	public MongoResultMessage error(String message) {
		body.put("status", "error");
		body.put("message", message);
		return this;
	}

	@Override
	public String address() {
		return null;
	}

	@Override
	public MultiMap headers() {
		return null;
	}

	@Override
	public JsonObject body() {
		return body;
	}

	@Override
	public String replyAddress() {
		return null;
	}

	@Override
	public boolean isSend() {
		return false;
	}

	@Override
	public void reply(Object message) {

	}

	@Override
	public <R> void reply(Object message, Handler<AsyncResult<Message<R>>> replyHandler) {

	}

	@Override
	public void reply(Object message, DeliveryOptions options) {

	}

	@Override
	public <R> void reply(Object message, DeliveryOptions options, Handler<AsyncResult<Message<R>>> replyHandler) {

	}

	@Override
	public void fail(int failureCode, String message) {

	}

}
