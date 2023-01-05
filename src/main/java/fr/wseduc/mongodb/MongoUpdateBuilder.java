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

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class MongoUpdateBuilder {

	private static final String SET = "$set";
	private static final String INC = "$inc";
	private static final String UNSET = "$unset";
	private static final String RENAME = "$rename";
	private static final String PUSH = "$push";
	private static final String PULL = "$pull";
	private static final String ADD_TO_SET = "$addToSet";

	private final JsonObject query;

	public MongoUpdateBuilder() {
		this.query = new JsonObject();
	}

	public MongoUpdateBuilder set(final String key, final Object value) {
		addToQuery(SET, key, value);
		return this;
	}

	public MongoUpdateBuilder addToSet(final String key, final Object value) {
		addToQuery(ADD_TO_SET, key, value);
		return this;
	}

	public MongoUpdateBuilder push(final String key, final Object value) {
		addToQuery(PUSH, key, value);
		return this;
	}

	public MongoUpdateBuilder push(final String key, final Object value, final int position) {
		final JsonObject valueInsert = new JsonObject()
				.put("$each", new JsonArray().add(value))
				.put("$position", position);
		addToQuery(PUSH, key, valueInsert);
		return this;
	}

	public MongoUpdateBuilder pull(final String key, final Object value) {
		addToQuery(PULL, key, value);
		return this;
	}

	public MongoUpdateBuilder rename(final String oldKey, final String newKey) {
		addToQuery(RENAME, oldKey, newKey);
		return this;
	}

	public MongoUpdateBuilder inc(final String key, final int value) {
		addToQuery(INC, key, value);
		return this;
	}

	public MongoUpdateBuilder unset(final String key) {
		addToQuery(UNSET, key, 1);
		return this;
	}

	public boolean isEmpty() {
		return query.size() == 0;
	}

	public JsonObject build() {
		return query;
	}

	private void addToQuery(String operator, String key, Object value) {
		JsonObject subquery = query.getJsonObject(operator);
		if (subquery == null) {
			subquery = new JsonObject();
			query.put(operator, subquery);
		}
		subquery.put(key, value);
	}

}
