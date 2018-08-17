package fr.wseduc.mongodb;

import java.util.Optional;

import com.mongodb.QueryBuilder;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class AggregationsBuilder {
	private final JsonObject object = new JsonObject();

	private AggregationsBuilder(String collection) {
		object.put("aggregate", collection);
		// object.put("action", "aggregate");
		object.put("pipeline", new JsonArray());
		// mandatory
		object.put("cursor", new JsonObject().put("batchSize", Integer.MAX_VALUE));
	}

	public static AggregationsBuilder startWithCollection(String collection) {
		AggregationsBuilder builder = new AggregationsBuilder(collection);
		return builder;
	}

	public AggregationsBuilder withGraphLookup(String startWith, String connectFromField, String connectToField,
			String as, Optional<Number> maxDepth, Optional<String> depthField,
			Optional<JsonObject> restrictSearchWithMatch) {
		return withGraphLookup(object.getString("aggregate"), startWith, connectFromField, connectToField, as, maxDepth,
				depthField, restrictSearchWithMatch);
	}

	public AggregationsBuilder withAllowDiskUse(boolean allowDiskUse) {
		this.object.put("allowDiskUse", allowDiskUse);
		return this;
	}

	public AggregationsBuilder withGraphLookup(String collection, String startWith, String connectFromField,
			String connectToField, String as, Optional<Number> maxDepth, Optional<String> depthField,
			Optional<JsonObject> restrictSearchWithMatch) {
		JsonObject graph = new JsonObject();
		graph.put("from", collection);
		graph.put("startWith", startWith);
		graph.put("connectFromField", connectFromField);
		graph.put("connectToField", connectToField);
		graph.put("as", as);
		if (maxDepth.isPresent())
			graph.put("maxDepth", maxDepth.get());
		if (depthField.isPresent())
			graph.put("depthField", depthField.get());
		if (restrictSearchWithMatch.isPresent())
			graph.put("restrictSearchWithMatch", restrictSearchWithMatch.get());

		JsonObject aggr = new JsonObject();
		aggr.put("$graphLookup", graph);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withMatch(QueryBuilder query) {
		JsonObject aggr = new JsonObject();
		aggr.put("$match", MongoQueryBuilder.build(query));

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withProjection(JsonObject projection) {
		JsonObject aggr = new JsonObject();
		aggr.put("$project", projection);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withAddFields(JsonObject projection) {
		JsonObject aggr = new JsonObject();
		aggr.put("$addFields", projection);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withCollStats(JsonObject collStat) {
		JsonObject aggr = new JsonObject();
		aggr.put("$collStats", collStat);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withCount(String field) {
		JsonObject aggr = new JsonObject();
		aggr.put("$count", field);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withGroup(JsonObject group) {
		JsonObject aggr = new JsonObject();
		aggr.put("$group", group);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withUnwind(String fieldPath) {
		JsonObject aggr = new JsonObject();
		aggr.put("$unwind", fieldPath);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withLimit(Integer limit) {
		JsonObject aggr = new JsonObject();
		aggr.put("$limit", limit);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withSkip(Integer limit) {
		JsonObject aggr = new JsonObject();
		aggr.put("$skip", limit);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withSort(JsonObject sorts) {
		JsonObject aggr = new JsonObject();
		aggr.put("$sort", sorts);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public AggregationsBuilder withLookup(String from, String localField, String foreignField, String as) {
		JsonObject look = new JsonObject();
		look.put("from", from);
		look.put("localField", localField);
		look.put("foreignField", foreignField);
		look.put("as", as);

		JsonObject aggr = new JsonObject();
		aggr.put("$lookup", look);

		this.object.getJsonArray("pipeline").add(aggr);
		return this;
	}

	public JsonObject getCommand() {
		return object;
	}
}
