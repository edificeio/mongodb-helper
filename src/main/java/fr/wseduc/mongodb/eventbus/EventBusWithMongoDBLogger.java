/*
 * Copyright © WebServices pour l'Éducation, 2014
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.wseduc.mongodb.eventbus;

import java.util.UUID;

import fr.wseduc.mongodb.MongoDb;
import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.eventbus.ReplyException;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;

public class EventBusWithMongoDBLogger implements EventBus {

	private static final Logger logger = LoggerFactory.getLogger(EventBusWithMongoDBLogger.class);
	private static final String LOGS_COLLECTION = "logs";
	private final EventBus eb;
	private final MongoDb mongo;

	public EventBusWithMongoDBLogger(Vertx vertx) {
		this.eb = vertx.eventBus();
		String node = (String) vertx.sharedData().getMap("server").get("node");
		if (node == null) {
			node = "";
		}
		this.mongo = MongoDb.getInstance();
		this.mongo.init(eb, node + "wse.mongodb.persistor");
	}

	private <T> JsonObject prepareLog(String address, T message) {
		JsonObject doc = new JsonObject();
		if (message instanceof JsonObject) {
			//doc.putObject("message", (JsonObject) message);
			doc.putString("message", ((JsonObject) message).encode());
		} else if (message instanceof JsonArray) {
			//doc.putArray("message", (JsonArray) message);
			doc.putString("message", ((JsonArray) message).encode());
		} else if (message instanceof Buffer) {
			doc.putString("message", "Buffer not displayed");
		} else {
			doc.putString("message", message.toString());
		}
		doc.putString("address", address)
		.putObject("date", MongoDb.now());
		return doc;
	}


	private <T> void sendLog(String address, T message) {
		JsonObject doc = prepareLog(address, message);
		doc.putString("type", "SEND");
		mongo.save(LOGS_COLLECTION, doc, MongoDb.WriteConcern.NONE, null);
	}

	private <T> void publishLog(String address, T message) {
		JsonObject doc = prepareLog(address, message);
		doc.putString("type", "PUBLISH");
		mongo.save(LOGS_COLLECTION, doc, MongoDb.WriteConcern.NONE, null);
	}

	private <T> String sendLogwithResponse(String address, T message) {
		String logMessageId = UUID.randomUUID().toString();
		JsonObject doc = prepareLog(address, message);
		doc.putString("_id", logMessageId)
		.putString("type", "SEND_WITH_REPLY");
		mongo.save(LOGS_COLLECTION, doc, MongoDb.WriteConcern.NONE, null);
		return logMessageId;
	}

	private <T> void responseLog(String logMessageId, T response) {
		JsonObject doc = new JsonObject();
		if (response instanceof JsonObject) {
			//doc.putObject("response", (JsonObject) response);
			doc.putString("response", ((JsonObject) response).encode());
		} else if (response instanceof JsonArray) {
			//doc.putArray("response", (JsonArray) response);
			doc.putString("response", ((JsonArray) response).encode());
		} else if (response instanceof Buffer) {
			doc.putString("response", "Buffer not displayed");
		} else {
			doc.putString("response", response.toString());
		}
		doc.putString("messageId", logMessageId)
		.putObject("date", MongoDb.now())
		.putString("type", "REPLY");
		mongo.save(LOGS_COLLECTION, doc, MongoDb.WriteConcern.NONE, null);
	}

	@Override
	public void close(Handler<AsyncResult<Void>> doneHandler) {
		eb.close(doneHandler);
	}

	@Override
	public EventBus send(String address, Object message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public EventBus send(String address, Object message,
			final Handler<Message> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		if (replyHandler != null) {
			return eb.send(address, message, new Handler<Message>() {

				@Override
				public void handle(Message event) {
					responseLog(logMessageId, event.body());
					replyHandler.handle(event);
				}
			});
		} else {
			return eb.send(address, message, replyHandler);
		}
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Object message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus send(String address, JsonObject message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, JsonObject message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	private <T> Handler<Message<T>> replyHandler(
			final Handler<Message<T>> replyHandler, final String logMessageId) {
		if (replyHandler == null) {
			return null;
		}
		return new Handler<Message<T>>() {

			@Override
			public void handle(Message<T> event) {
				responseLog(logMessageId, event.body());
				replyHandler.handle(event);
			}
		};
	}

	private <T> Handler<AsyncResult<Message<T>>> timeoutReplyHandler(
			final Handler<AsyncResult<Message<T>>> replyHandler, final String logMessageId) {
		if (replyHandler == null) {
			return null;
		}
		return new Handler<AsyncResult<Message<T>>>() {
			@Override
			public void handle(AsyncResult<Message<T>> event) {
				if (event.succeeded()) {
					responseLog(logMessageId, event.result().body());
				} else {
					ReplyException ex = (ReplyException)event.cause();
					logger.error("MessageId : " + logMessageId);
					logger.error("Failure type: " + ex.failureType());
					logger.error("Failure code: " + ex.failureCode());
					logger.error("Failure message: " + ex.getMessage());
				}
				replyHandler.handle(event);
			}
		};
	}

	@Override
	public EventBus send(String address, JsonObject message) {
		sendLog(address, message);
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, JsonArray message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, JsonArray message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, JsonArray message) {
		sendLog(address, message);
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Buffer message,
			final Handler<Message<T>> replyHandler) {
//		final String logMessageId = sendLogwithResponse(address, message.toString());
//		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
		return eb.send(address, message, replyHandler);
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Buffer message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		return eb.sendWithTimeout(address, message, timeout, replyHandler);
	}

	@Override
	public EventBus send(String address, Buffer message) {
		//sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, byte[] message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, new String(message));
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, byte[] message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, new String(message));
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, byte[] message) {
		sendLog(address, new String(message));
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, String message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, String message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message);
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, String message) {
		sendLog(address, message);
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Integer message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Integer message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Integer message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Long message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Long message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Long message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Float message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Float message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Float message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Double message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Double message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Double message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Boolean message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Boolean message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Boolean message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Short message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Short message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Short message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Character message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Character message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Character message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public <T> EventBus send(String address, Byte message,
			final Handler<Message<T>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.send(address, message, replyHandler(replyHandler, logMessageId));
	}

	@Override
	public <T> EventBus sendWithTimeout(String address, Byte message, long timeout,
			Handler<AsyncResult<Message<T>>> replyHandler) {
		final String logMessageId = sendLogwithResponse(address, message.toString());
		return eb.sendWithTimeout(address, message, timeout,
				timeoutReplyHandler(replyHandler, logMessageId));
	}

	@Override
	public EventBus send(String address, Byte message) {
		sendLog(address, message.toString());
		return eb.send(address, message);
	}

	@Override
	public EventBus publish(String address, Object message) {
		publishLog(address, message);
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, JsonObject message) {
		publishLog(address, message);
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, JsonArray message) {
		publishLog(address, message);
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Buffer message) {
		//publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, byte[] message) {
		publishLog(address, new String(message));
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, String message) {
		publishLog(address, message);
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Integer message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Long message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Float message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Double message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Boolean message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Short message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Character message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus publish(String address, Byte message) {
		publishLog(address, message.toString());
		return eb.publish(address, message);
	}

	@Override
	public EventBus unregisterHandler(String address,
			Handler<? extends Message> handler,
			Handler<AsyncResult<Void>> resultHandler) {
		return eb.unregisterHandler(address, handler, resultHandler);
	}

	@Override
	public EventBus unregisterHandler(String address,
			Handler<? extends Message> handler) {
		return eb.unregisterHandler(address, handler);
	}

	@Override
	public EventBus registerHandler(String address,
			Handler<? extends Message> handler,
			Handler<AsyncResult<Void>> resultHandler) {
		return eb.registerHandler(address, handler, resultHandler);
	}

	@Override
	public EventBus registerHandler(String address,
			Handler<? extends Message> handler) {
		return eb.registerHandler(address, handler);
	}

	@Override
	public EventBus registerLocalHandler(String address,
			Handler<? extends Message> handler) {
		return eb.registerLocalHandler(address, handler);
	}

	@Override
	public EventBus setDefaultReplyTimeout(long timeoutMs) {
		return eb.setDefaultReplyTimeout(timeoutMs);
	}

	@Override
	public long getDefaultReplyTimeout() {
		return eb.getDefaultReplyTimeout();
	}

}
