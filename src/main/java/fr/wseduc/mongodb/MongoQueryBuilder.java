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

import io.vertx.core.json.JsonObject;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

public class MongoQueryBuilder {

  private static final JsonWriterSettings jws = JsonWriterSettings.builder()
    .outputMode(JsonMode.STRICT)
    .build();
	public static JsonObject build(Bson queryBuilder) {
			return new JsonObject(queryBuilder.toBsonDocument().toJson(jws));
	}

}
