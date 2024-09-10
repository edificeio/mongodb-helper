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
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import com.mongodb.DBObject;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public interface MongoDbAPI {

	public static final String ISO_DATE_FORMAT = "yyyy-MM-dd HH:mm.ss.SSS";

	public static enum WriteConcern {
		NONE, NORMAL, SAFE, MAJORITY, FSYNC_SAFE, JOURNAL_SAFE, REPLICAS_SAFE;
	}

	public void init(EventBus eb, String address);

	public void save(String collection, JsonObject document, WriteConcern writeConcern,
			final Handler<Message<JsonObject>> callback);

	public void save(String collection, JsonObject document, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, final Handler<Message<JsonObject>> callback);

	public void save(String collection, JsonObject document, Handler<Message<JsonObject>> callback);
	public void save(String collection, JsonObject document);

	public void insert(String collection, JsonArray documents, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback);

	public void insert(String collection, JsonArray documents, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void insert(String collection, JsonArray documents, Handler<Message<JsonObject>> callback);

	public void insert(String collection, JsonObject document, Handler<Message<JsonObject>> callback);

	public void insert(String collection, JsonArray documents);

	public void insert(String collection, JsonObject document);

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
			WriteConcern writeConcern, Handler<Message<JsonObject>> callback);

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
			WriteConcern writeConcern, DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi,
			Handler<Message<JsonObject>> callback);

	public void update(String collection, JsonObject criteria, JsonObject objNew, boolean upsert, boolean multi);

	public void update(String collection, JsonObject criteria, JsonObject objNew);

	public void update(String collection, JsonObject criteria, JsonObject objNew,
			Handler<Message<JsonObject>> callback);

	public void update(String collection, JsonObject criteria, JsonObject objNew, JsonArray arrayFilters);

	public void update(String collection, JsonObject criteria, JsonObject objNew, JsonArray arrayFilters,
					   Handler<Message<JsonObject>> callback);

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys, int skip,
			int limit, int batchSize, Handler<Message<JsonObject>> callback);

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys, int skip,
			int limit, int batchSize, DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void find(String collection, JsonObject matcher, JsonObject sort, JsonObject keys,
			Handler<Message<JsonObject>> callback);

	public void find(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback);

	public void findOne(String collection, JsonObject matcher, JsonObject keys, JsonArray fetch,
			Handler<Message<JsonObject>> callback);

	public void findOne(String collection, JsonObject matcher, JsonObject keys, JsonArray fetch,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void findOne(String collection, JsonObject matcher, JsonObject keys, Handler<Message<JsonObject>> callback);

	public void findOne(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback);

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, Handler<Message<JsonObject>> callback);

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, boolean remove, boolean returnNew, boolean upsert,
			Handler<Message<JsonObject>> callback);

	public void findAndModify(String collection, JsonObject matcher, JsonObject update, JsonObject sort,
			JsonObject fields, boolean remove, boolean returnNew, boolean upsert,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void count(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback);

	public void distinct(String collection, String key, JsonObject matcher, Handler<Message<JsonObject>> callback);

	public void distinct(String collection, String key, Handler<Message<JsonObject>> callback);

	public void delete(String collection, JsonObject matcher, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback);

	public void delete(String collection, JsonObject matcher, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void delete(String collection, JsonObject matcher, Handler<Message<JsonObject>> callback);

	public void delete(String collection, JsonObject matcher);

	public void bulk(String collection, JsonArray commands, Handler<Message<JsonObject>> callback);

	public void bulk(String collection, JsonArray commands, WriteConcern writeConcern,
			Handler<Message<JsonObject>> callback);

	public void bulk(String collection, JsonArray commands, WriteConcern writeConcern,
			DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void command(String command, Handler<Message<JsonObject>> callback);

	public void command(String command, DeliveryOptions deliveryOptions, Handler<Message<JsonObject>> callback);

	public void aggregate(JsonObject command, final Handler<Message<JsonObject>> handler);

	static boolean isOk(JsonObject body) {
		return "ok".equals(body.getString("status"));
	}

	public void aggregateBatched(String collection, JsonObject command, int maxBatch, final Handler<Message<JsonObject>> handler);

	public void command(String command);

	public void getCollections(Handler<Message<JsonObject>> callback);

	public void getCollectionStats(String collection, Handler<Message<JsonObject>> callback);

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

	public static JsonObject offsetFromNow(long offsetInSeconds)
	{
		return new JsonObject().put("$date", System.currentTimeMillis() + (offsetInSeconds * 1000));
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

}
