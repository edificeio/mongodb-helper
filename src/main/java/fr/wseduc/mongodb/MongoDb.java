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

package fr.wseduc.mongodb;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import com.mongodb.ReadPreference;
import io.vertx.core.Promise;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class MongoDb implements MongoDbAPI {

	private EventBus eb;
	private String address;

	public static final DateTimeFormatter dtf = DateTimeFormatter.ISO_DATE;
	public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");


	private MongoDb() {
	}

	private static class MongoDbHolder {
		private static final MongoDb instance = new MongoDb();
	}

	public static MongoDb getInstance() {
		return MongoDbHolder.instance;
	}

	public void init(EventBus eb, String address) {
		this.eb = eb;
		this.address = address;
	}

	public boolean isInitialized() {
		return this.eb != null && this.address != null;
	}

	public void save(String collection, JsonObject document, WriteConcern writeConcern,
			final Handler<Message<JsonObject>> callback) {
		save(collection, document, writeConcern, null, callback);
	}

	public void save(String collection, JsonObject document, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, final Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "save");
		jo.put("collection", collection);
		jo.put("document", document);
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		sendRequest(jo, deliveryOptions, callback);
	}

	public void save(String collection, JsonObject document, Handler<Message<JsonObject>> callback) {
		save(collection, document, null, callback);
	}

	public void save(String collection, JsonObject document) {
		save(collection, document, null, null);
	}

	public void insert(String collection, JsonArray documents, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback) {
		insert(collection, documents, writeConcern, null, callback);
	}

	public void insert(String collection, JsonArray documents, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "insert");
		jo.put("collection", collection);
		if (documents.size() > 1) {
			jo.put("documents", documents);
			jo.put("multiple", true);
		} else {
			jo.put("document", documents.getJsonObject(0));
		}
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		sendRequest(jo, deliveryOptions, callback);
	}

	public void insert(String collection, JsonArray documents, Handler<Message<JsonObject>> callback) {
		insert(collection, documents, null, callback);
	}

	public void insert(String collection, JsonObject document, Handler<Message<JsonObject>> callback) {
		insert(collection, new JsonArray().add(document), null, callback);
	}

	public void insert(String collection, JsonArray documents) {
		insert(collection, documents, null, null);
	}

	public void insert(String collection, JsonObject document) {
		insert(collection, new JsonArray().add(document), null, null);
	}

	/**
	 *
	 * @param collection
	 * @param criteria     the query argument corresponds to the WHERE statement
	 * @param objNew       the update corresponds to the SET ... statement
	 * @param upsert       if true and document doesn't exist, save it
	 * @param multi        update all document matching query
	 * @param arrayFilters the filter for array updates
	 * @param writeConcern
	 * @param callback
	 */
	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
					   JsonArray arrayFilters, WriteConcern writeConcern, Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, upsert, multi, arrayFilters, writeConcern, null, callback);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
					   JsonArray arrayFilters, WriteConcern writeConcern, DeliveryOptions deliveryOptions,
					   Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "update");
		jo.put("collection", collection);
		jo.put("criteria", criteria);
		jo.put("objNew", objNew);
		jo.put("upsert", upsert);
		jo.put("multi", multi);
		jo.put("arrayFilters", arrayFilters);

		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}

		sendRequest(jo, deliveryOptions, callback);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
			WriteConcern writeConcern, Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, upsert, multi, null, writeConcern, null, callback);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
			WriteConcern writeConcern, DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, upsert, multi, null, writeConcern, deliveryOptions, callback);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
			Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, upsert, multi, null, callback);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi) {
		update(collection, criteria, objNew, upsert, multi, null, null);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew) {
		update(collection, criteria, objNew, false, false, null, null);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew,
			Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, false, false, null, callback);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew, JsonArray arrayFilters) {
		update(collection, criteria, objNew, false, false, arrayFilters, null, null);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew, JsonArray arrayFilters,
					   Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, false, false, arrayFilters,null, callback);
	}

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys, int skip,
			int limit, int batchSize, Handler<Message<JsonObject>> callback) {
		find(collection, matcher, sort, keys, skip, limit, batchSize, null, callback);
	}

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys, int skip,
					 int limit, int batchSize, DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback) {
		find(collection, matcher, sort, keys, skip, limit, batchSize, deliveryOptions, null, callback);
	}

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys, int skip,
			int limit, int batchSize, DeliveryOptions deliveryOptions, ReadPreference readPreference, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "find");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		jo.put("sort", sort);
		jo.put("keys", keys);
		jo.put("skip", skip);
		jo.put("limit", limit);
		jo.put("batch_size", batchSize);
		if(readPreference != null){
			jo.put("read_preference", readPreference.getName());
		}
		sendRequest(jo, deliveryOptions, callback);
	}

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys,
			Handler<Message<JsonObject>> callback) {
		find(collection, matcher, sort, keys, -1, -1, Integer.MAX_VALUE, callback);
	}

	public void find(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback) {
		find(collection, matcher, null, null, -1, -1, Integer.MAX_VALUE, callback);
	}

	public void findOne(String collection, JsonObject matcher, JsonObject keys, JsonArray fetch,
			Handler<Message<JsonObject>> callback) {
		findOne(collection, matcher, keys, fetch, null, callback);
	}

	public void findOne(String collection, JsonObject matcher, JsonObject keys, JsonArray fetch, DeliveryOptions deliveryOptions,
						Handler<Message<JsonObject>> callback) {
		findOne(collection, matcher, keys, fetch, deliveryOptions, null, callback);
	}

	public void findOne(String collection, JsonObject matcher, JsonObject keys, JsonArray fetch,
			DeliveryOptions deliveryOptions, ReadPreference readPreference, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "findone");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		jo.put("keys", keys);
		if(readPreference != null){
			jo.put("read_preference", readPreference.getName());
		}
		if (fetch != null) {
			jo.put("fetch", fetch);
		}
		sendRequest(jo, deliveryOptions, callback);
	}

	public void findOne(String collection, JsonObject matcher, JsonObject keys, Handler<Message<JsonObject>> callback) {
		findOne(collection, matcher, keys, null, callback);
	}

	public void findOne(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback) {
		findOne(collection, matcher, null, callback);
	}

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, Handler<Message<JsonObject>> callback) {
		findAndModify(collection, matcher, update, sort, fields, false, false, false, callback);
	}

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, boolean remove, boolean returnNew, boolean upsert,
			Handler<Message<JsonObject>> callback) {
		findAndModify(collection, matcher, update, sort, fields, remove, returnNew, upsert, null, callback);
	}

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, boolean remove, boolean returnNew, boolean upsert,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "find_and_modify");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		jo.put("update", update);
		jo.put("sort", sort);
		jo.put("fields", fields);
		jo.put("remove", remove);
		jo.put("new", returnNew);
		jo.put("upsert", upsert);
		sendRequest(jo, deliveryOptions, callback);
	}

	public void count(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback) {
		count(collection, matcher, null, callback);
	}

	public void count(String collection, JsonObject matcher, ReadPreference readPreference, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "count");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		if(readPreference != null){
			jo.put("read_preference", readPreference.getName());
		}
		sendRequest(jo, null, callback);
	}

	public void distinct(String collection, String key, final JsonObject matcher, final String resultClassname,
											 final Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "distinct");
		jo.put("collection", collection);
		jo.put("key", key);
		jo.put("matcher", matcher);
		jo.put("resultClassname", resultClassname);
		sendRequest(jo, null, callback);
	}

	public void distinct(String collection, String key, JsonObject matcher, Handler<Message<JsonObject>> callback) {
		distinct(collection, key, matcher, null, callback);
	}

	public void distinct(String collection, String key, Handler<Message<JsonObject>> callback) {
		distinct(collection, key, null, callback);
	}

	public void delete(String collection, JsonObject matcher, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback) {
		delete(collection, matcher, writeConcern, null, callback);
	}

	public void delete(String collection, JsonObject matcher, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "delete");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		sendRequest(jo, deliveryOptions, callback);
	}

	public void delete(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback) {
		delete(collection, matcher, null, callback);
	}

	public void delete(String collection, JsonObject matcher) {
		delete(collection, matcher, null, null);
	}

	public void bulk(String collection, JsonArray commands, Handler<Message<JsonObject>> callback) {
		bulk(collection, commands, null, callback);
	}

	public void bulk(String collection, JsonArray commands, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback) {
		bulk(collection, commands, writeConcern, null, callback);
	}

	public void bulk(String collection, JsonArray commands, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "bulk");
		jo.put("collection", collection);
		jo.put("commands", commands);
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		sendRequest(jo, deliveryOptions, callback);
	}

	public void command(String command, Handler<Message<JsonObject>> callback) {
		command(command, null, callback);
	}

	public void command(String command, DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "command");
		jo.put("command", command);
		sendRequest(jo, deliveryOptions, callback);
	}

	public void aggregate(JsonObject command, final Handler<Message<JsonObject>> handler) {
		this.command(command.toString(), handler);
	}

	/**
	 * Calls aggregation pipeline on a collection.
	 * @param collection Name of the collection on which the aggregate function should
	 *                   be called
	 * @param pipelines Specification of the pipelines to execute
	 * @return Results of the operation
	 */
	public Future<JsonArray> aggregate(final String collection,
												 final JsonArray pipelines) {
		final JsonObject jo = new JsonObject()
		.put("action", "aggregate")
		.put("collection", collection)
		.put("pipelines", pipelines);
		final Promise<JsonArray> promise = Promise.promise();
		eb.request(address, jo, event -> {
			if (event.succeeded()) {
				final JsonObject body = (JsonObject) event.result().body();
				if(isOk(body)) {
					promise.complete(body.getJsonArray("results"));
				} else {
					promise.fail(body.getString("message"));
				}
			} else {
				promise.fail(event.cause().getMessage());
			}
		});
		return promise.future();
	}

	public static boolean isOk(JsonObject body) {
		return "ok".equals(body.getString("status"));
	}
	public static String toErrorStr(JsonObject body) {
		return body.getString("error", body.getString("message", "query helper error"));
	}

	public void getNextBatch(String collection, Long cursorId, final Handler<Message<JsonObject>> handler) {
		getNextBatch(collection, cursorId, Integer.MAX_VALUE, handler);
	}

	public void getNextBatch(String collection, Long cursorId, int batchSize, final Handler<Message<JsonObject>> handler){
		JsonObject command = new JsonObject();
		command.put("getMore", cursorId);
		command.put("collection", collection);
		command.put("batchSize", batchSize);
		//
		JsonObject jo = new JsonObject();
		jo.put("action", "command");
		jo.put("command", command.toString());
		sendRequest(jo, handler);
	}

	public void aggregateBatched(String collection, JsonObject command, int maxBatch, final Handler<Message<JsonObject>> handler) {
		aggregate(command, message -> {
			final JsonObject body = message.body();
			if(isOk(body)){
				final JsonObject result = body.getJsonObject("result", new JsonObject());
				final JsonObject cursor = result.getJsonObject("cursor", new JsonObject());
				final JsonArray firstBatch = cursor.getJsonArray("firstBatch", new JsonArray());
				cursor.put("firstBatch", firstBatch);//put if not exists yet
				Future<JsonObject> future = Future.succeededFuture(cursor);
				for(int i = 0; i < maxBatch; i++){
					future = future.compose((previous)->{
						Long cursorId = previous.getLong("id",0l);
						Promise<JsonObject> next = Promise.promise();
						if(cursorId > 0) {
							getNextBatch(collection, cursorId, nextMsg->{
								JsonObject nextBody = nextMsg.body();
								if(isOk(nextBody)) {
									final JsonObject nextCursor = nextBody.getJsonObject("result", new JsonObject()).getJsonObject("cursor", new JsonObject());
									final JsonArray nextBatch = nextCursor.getJsonArray("nextBatch", new JsonArray());
									firstBatch.addAll(nextBatch);
									next.complete(nextCursor);
								}else{
									next.complete(new JsonObject());
								}
							});
						} else {
							next.complete(new JsonObject());
						}
						//
						return next.future();
					});
				}
				future.onComplete(res->{
					handler.handle(new MongoResultMessage(body));
				});
			}else{
				handler.handle(message);
			}
		});
	}

	public void command(String command) {
		command(command, null);
	}

	public void getCollections(Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "getCollections");
		sendRequest(jo, callback);
	}

	public void getCollectionStats(String collection, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "collectionStats");
		jo.put("collection", collection);
		sendRequest(jo, callback);
	}

	public static String formatDate(Date date) {
		DateFormat df = new SimpleDateFormat(ISO_DATE_FORMAT);
		return df.format(date);
	}

	public static Date parseDate(String date) throws ParseException {
		DateFormat df = new SimpleDateFormat(ISO_DATE_FORMAT);
		return df.parse(date);
	}

	public static JsonObject now() {
		return nowISO();
	}

	public static JsonObject toMongoDate(final Date date) {
		return toMongoDateISO(date);
	}

	public static JsonObject toMongoDate(final LocalDateTime date) {
		return toMongoDateISO(date);
	}

	public static JsonObject nowISO() {
		return new JsonObject().put("$date", OffsetDateTime.ofInstant(Instant.now(), ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
	}

	public static JsonObject toMongoDateISO(final Date date) {
		return new JsonObject().put("$date", OffsetDateTime.ofInstant(Instant.ofEpochMilli(date.getTime()), ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
	}

	public static JsonObject toMongoDateISO(final LocalDateTime date) {
		return new JsonObject().put("$date", OffsetDateTime.ofInstant(date.toInstant(ZoneOffset.UTC), ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
	}

	public static JsonObject offsetFromNow(long offsetInSeconds)
	{
		return toMongoDateISO(new Date(System.currentTimeMillis() + (offsetInSeconds * 1000)));
	}

	public static Date parseIsoDate(JsonObject date) {
		Object d = date.getValue("$date");
		if (d instanceof Long) {
			return new Date((Long) d);
		} else {
			return Date.from(OffsetDateTime.parse((String)d).toInstant());
    }
	}

	private Handler<AsyncResult<Message<JsonObject>>> getAdapterHandler(final Handler<Message<JsonObject>> callback) {
		if (callback == null)
			return null;
		return event -> {
      if (event.succeeded()) {
        callback.handle(event.result());
      } else {
        callback.handle(new MongoResultMessage().error(event.cause().getMessage()));
      }
    };
	}

	private Future<Message<JsonObject>> sendRequest(final JsonObject payload,
																									final Handler<Message<JsonObject>> callback) {
		return sendRequest(payload, null, callback);
	}

	private Future<Message<JsonObject>> sendRequest(final JsonObject payload,
																									final DeliveryOptions deliveryOptions,
																									final Handler<Message<JsonObject>> callback) {
		Future<Message<JsonObject>> future;
		Handler<AsyncResult<Message<JsonObject>>> handler;
		if (deliveryOptions == null) {
			future = eb.request(address, payload);
			handler = getAdapterHandler(callback);
		} else {
			future = eb.request(address, payload, deliveryOptions);
			handler = getAdapterHandler(callback);
		}
		return future.onComplete(e -> {
			if(handler != null) {
				handler.handle(e);
			}
		});
	}
}
