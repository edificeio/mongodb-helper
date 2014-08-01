package fr.wseduc.mongodb;

import org.vertx.java.core.json.JsonObject;

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
		JsonObject subquery = query.getObject(operator);
		if (subquery == null) {
			subquery = new JsonObject();
			query.putObject(operator, subquery);
		}
		subquery.putValue(key, value);
	}

}
