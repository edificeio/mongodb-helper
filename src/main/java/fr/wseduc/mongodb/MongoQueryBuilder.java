package fr.wseduc.mongodb;

import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;
import com.mongodb.util.JSON;
import org.vertx.java.core.json.JsonObject;

public class MongoQueryBuilder {

	public static JsonObject build(QueryBuilder queryBuilder) {
		DBObject dbo = queryBuilder.get();
		return new JsonObject(JSON.serialize(dbo));
	}

}
