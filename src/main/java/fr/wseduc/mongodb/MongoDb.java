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
import java.util.Calendar;
import java.util.Date;

import javax.xml.bind.DatatypeConverter;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class MongoDb {

	private static final String ISO_DATE_FORMAT = "yyyy-MM-dd HH:mm.ss.SSS";
	private EventBus eb;
	private String address;

	public static enum WriteConcern {
		NONE, NORMAL, SAFE, MAJORITY, FSYNC_SAFE, JOURNAL_SAFE, REPLICAS_SAFE;
	}

	private MongoDb() {}

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

	public void save(String collection, JsonObject document, WriteConcern writeConcern,
			final Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "save");
		jo.put("collection", collection);
		jo.put("document", document);
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void save(String collection, JsonObject document,
			Handler<Message<JsonObject>> callback) {
		save(collection, document, null, callback);
	}

	public void save(String collection, JsonObject document) {
		save(collection, document, null, null);
	}

	public void insert(String collection, JsonArray documents, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback) {
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
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void insert(String collection, JsonArray documents,
			Handler<Message<JsonObject>> callback) {
		insert(collection, documents, null, callback);
	}

	public void insert(String collection, JsonObject document,
			Handler<Message<JsonObject>> callback) {
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
	 * @param criteria the query argument corresponds to the WHERE statement
	 * @param objNew the update corresponds to the SET ... statement
	 * @param upsert if true and document doesn't exist, save it
	 * @param multi update all document matching query
	 * @param writeConcern
	 * @param callback
	 */
	public void update(String collection, JsonObject criteria, JsonObject objNew,
			boolean upsert, boolean multi, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "update");
		jo.put("collection", collection);
		jo.put("criteria", criteria);
		jo.put("objNew", objNew);
		jo.put("upsert", upsert);
		jo.put("multi", multi);
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew,
			boolean upsert, boolean multi, Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, upsert, multi, null, callback);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew,
			boolean upsert, boolean multi) {
		update(collection, criteria, objNew, upsert, multi, null, null);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew) {
		update(collection, criteria, objNew, false, false, null, null);
	}

	public void update(String collection, JsonObject criteria, JsonObject objNew,
			Handler<Message<JsonObject>> callback) {
		update(collection, criteria, objNew, false, false, null, callback);
	}

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys,
			int skip, int limit, int batchSize, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "find");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		jo.put("sort", sort);
		jo.put("keys", keys);
		jo.put("skip", skip);
		jo.put("limit", limit);
		jo.put("batch_size", batchSize);
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys,
			Handler<Message<JsonObject>> callback) {
		find(collection, matcher, sort, keys, -1, -1, Integer.MAX_VALUE, callback);
	}

	public void find(String collection, JsonObject matcher,
			Handler<Message<JsonObject>> callback) {
		find(collection, matcher, null, null, -1, -1, Integer.MAX_VALUE, callback);
	}

	public void findOne(String collection, JsonObject matcher, JsonObject keys, JsonArray fetch,
			Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "findone");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		jo.put("keys", keys);
		if (fetch != null) {
			jo.put("fetch", fetch);
		}
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void findOne(String collection, JsonObject matcher, JsonObject keys,
						Handler<Message<JsonObject>> callback) {
		findOne(collection, matcher, keys, null, callback);
	}

	public void findOne(String collection, JsonObject matcher,
			Handler<Message<JsonObject>> callback) {
		findOne(collection, matcher, null, callback);
	}

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, Handler<Message<JsonObject>> callback) {
		findAndModify(collection, matcher, update, sort, fields, false, false, false, callback);
	}

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, boolean remove, boolean returnNew, boolean upsert, Handler<Message<JsonObject>> callback) {
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
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void count(String collection, JsonObject matcher,
			Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "count");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void distinct(String collection, String key, JsonObject matcher, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "distinct");
		jo.put("collection", collection);
		jo.put("key", key);
		jo.put("matcher", matcher);
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void distinct(String collection, String key, Handler<Message<JsonObject>> callback) {
		distinct(collection, key, null, callback);
	}

	public void delete(String collection, JsonObject matcher, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "delete");
		jo.put("collection", collection);
		jo.put("matcher", matcher);
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void delete(String collection, JsonObject matcher,
			Handler<Message<JsonObject>> callback) {
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
		JsonObject jo = new JsonObject();
		jo.put("action", "bulk");
		jo.put("collection", collection);
		jo.put("commands", commands);
		if (writeConcern != null) {
			jo.put("write_concern", writeConcern.name());
		}
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void command(String command, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "command");
		jo.put("command", command);
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void command(String command) {
		command(command, null);
	}

	public void getCollections(Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "getCollections");
		eb.send(address, jo, getAdapterHandler(callback));
	}

	public void getCollectionStats(String collection, Handler<Message<JsonObject>> callback) {
		JsonObject jo = new JsonObject();
		jo.put("action", "collectionStats");
		jo.put("collection", collection);
		eb.send(address, jo, getAdapterHandler(callback));
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
		return new JsonObject().put("$date", System.currentTimeMillis());
	}

	public static Date parseIsoDate(JsonObject date) {
		Object d = date.getValue("$date");
		if (d instanceof Long) {
			return new Date((Long) d);
		} else {
			Calendar c = DatatypeConverter.parseDateTime((String) d);
			return c.getTime();
		}
	}

	private Handler<AsyncResult<Message<JsonObject>>> getAdapterHandler(final Handler<Message<JsonObject>> callback) {
		if (callback == null) return null;
		return new Handler<AsyncResult<Message<JsonObject>>>() {
			@Override
			public void handle(AsyncResult<Message<JsonObject>> event) {
				if (event.succeeded()) {
					callback.handle(event.result());
				} else {
					callback.handle(new MongoResultMessage().error(event.cause().getMessage()));
				}
			}
		};
	}

}
