package fr.wseduc.mongodb.integration;

import java.util.Optional;

import com.mongodb.client.model.Filters;
import org.junit.Test;

import fr.wseduc.mongodb.AggregationsBuilder;
import io.vertx.core.json.JsonObject;
import junit.framework.Assert;

public class AggregatorTest {

	@Test
	public void shouldGenerateGraphLookup() {
		final String aggregationQuery = "{\n" + //
				"	\"aggregate\":\"test\",\n" + //
				"	\"pipeline\":[\n" + //
				"		{\n" + //
				"			\"$graphLookup\": {\n" + //
				"		         \"from\": \"test\",\n" + //
				"		         \"startWith\": \"uid1\",\n" + //
				"		         \"connectFromField\": \"id\",\n" + //
				"		         \"connectToField\": \"parentId\",\n" + //
				"		         \"as\": \"tree\",\n" + //
				"		         \"maxDepth\":5,\n" + //
				"		         \"depthField\":\"depth\",\n" + //
				"		         \"restrictSearchWithMatch\":{\"fname\":\"nabil\"}\n" + //
				"		      }\n" + //
				"	      },\n" + //
				"	     {\"$limit\":5},\n" + //
				"	     { \"$match\": { \"test\":\"test\" } },\n" + //
				"	     { \"$project\": { \"title\":1 } }\n" + //
				"	],\n" + //
				"\"cursor\":{\"batchSize\":2147483647},\"allowDiskUse\":true\n" + //
				"}";
		JsonObject command = AggregationsBuilder.startWithCollection("test").withAllowDiskUse(true)//
				.withGraphLookup("uid1", "id", "parentId", "tree", Optional.of(5), Optional.of("depth"),
						Optional.of(new JsonObject().put("fname", "nabil")))//
				.withLimit(5)//
				.withMatch(Filters.eq("test", "test"))//
				.withProjection(new JsonObject().put("title", 1))//
				.getCommand();
		Assert.assertEquals(new JsonObject(aggregationQuery), command);
	}

	@Test
	public void shouldGenerateGroupQuery() {
		final String aggregationQuery = "{\n" + //
				"	\"aggregate\":\"test\",\n" + //
				"	\"allowDiskUse\":true,\"cursor\":{\"batchSize\":2147483647},\n" + //
				"	\"pipeline\":[\n" + //
				"		 {\"$group\":{ \"_id\" : \"notifiedUsers\"}},\n" + //
				"	     {\"$unwind\":\"$recipients\"},\n" + //
				"	     { \"$match\": { \"test\":\"test\" } },\n" + //
				"	     { \"$project\": { \"title\":1 } }\n" + //
				"	]\n" + //
				"}";
		JsonObject command = AggregationsBuilder.startWithCollection("test").withAllowDiskUse(true)//
				.withGroup(new JsonObject().put("_id", "notifiedUsers"))//
				.withUnwind("$recipients")//
				.withMatch(Filters.eq("test", "test"))//
				.withProjection(new JsonObject().put("title", 1))//
				.getCommand();
		Assert.assertEquals(new JsonObject(aggregationQuery), command);
	}
}
