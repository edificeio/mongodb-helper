///*
// * Copyright © WebServices pour l'Éducation, 2014
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package fr.wseduc.mongodb.integration;
//
//import static io.vertx.testtools.VertxAssert.assertEquals;
//import static io.vertx.testtools.VertxAssert.testComplete;
//
//import org.junit.Test;
//import io.vertx.core.AsyncResult;
//import io.vertx.core.Handler<AsyncResult>;
//import io.vertx.core.Handler;
//import io.vertx.core.eventbus.EventBus;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.json.JsonObject;
//import io.vertx.testtools.TestVerticle;
//
//import fr.wseduc.mongodb.MongoDb;
//
//public class MongoDbTest extends TestVerticle {
//
//	private MongoDb mongo;
//
//	@Override
//	public void start() {
//		EventBus eb = vertx.eventBus();
//		JsonObject config = new JsonObject();
//		config.put("address", "test.persistor");
//		config.put("db_name", System.getProperty("vertx.mongo.database", "test_db"));
//		config.put("host", System.getProperty("vertx.mongo.host", "localhost"));
//		config.put("port", Integer.valueOf(System.getProperty("vertx.mongo.port", "27017")));
//		String username = System.getProperty("vertx.mongo.username");
//		String password = System.getProperty("vertx.mongo.password");
//		if (username != null) {
//			config.put("username", username);
//			config.put("password", password);
//		}
//		config.put("fake", false);
//		container.deployModule("io.vertx~mod-mongo-persistor~2.0.0-CR1", config, 1, new Handler<AsyncResult><String>() {
//			public void handle(AsyncResult<String> ar) {
//				if (ar.succeeded()) {
//					MongoDbTest.super.start();
//				} else {
//					ar.cause().printStackTrace();
//				}
//			}
//		});
//		mongo = MongoDb.getInstance();
//		mongo.init(eb, "test.persistor");
//	}
//
//	@Test
//	public void testPersistor() throws Exception {
//		JsonObject document = new JsonObject();
//		document.put("content", "blip");
//		mongo.save("test", document, new Handler<Message<JsonObject>>() {
//			@Override
//			public void handle(Message<JsonObject> msg) {
//				String id = msg.body().getString("_id");
//				String query = "{\"_id\":\"" + id + "\"}";
//				String set = "{\"$set\": { \"content\": \"blop\"}}";
//				mongo.update("test", new JsonObject(query), new JsonObject(set));
//				mongo.findOne("test", new JsonObject(query), new Handler<Message<JsonObject>>() {
//					@Override
//					public void handle(Message<JsonObject> res) {
//						container.logger().info(res.body().toString());
//						assertEquals("blop", res.body().getJsonObject("result").getString("content"));
//						testComplete();
//					}
//				});
//			}
//		});
//	}
//
//	@Test
//	public void testCommand() throws Exception {
//		mongo.command("{ping:1}", new Handler<Message<JsonObject>>() {
//			public void handle(Message<JsonObject> reply) {
//				Number ok = reply.body().getJsonObject("result").getNumber("ok");
//				assertEquals(0.0, ok);
//				testComplete();
//			}
//		});
//	}
//
//}
