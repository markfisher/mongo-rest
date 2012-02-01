/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.services.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Generic REST API for MongoDB collections.
 */
@Controller
@RequestMapping(produces = "application/json")
public class DataController {

	private final MongoTemplate mongoTemplate;

	private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<String, AtomicLong>();


	@Autowired
	public DataController(MongoTemplate mongoTemplate) {
		Assert.notNull(mongoTemplate, "mongoTemplate must not be null");
		this.mongoTemplate = mongoTemplate;
		// clean slate upon startup for now...
		this.init();
	}

	private void init() {
		for (String collection : this.mongoTemplate.getCollectionNames()) {
			if (!collection.contains("system")) {
				this.mongoTemplate.dropCollection(collection);
			}
		}
	}

	@RequestMapping(value = "/", method = RequestMethod.GET)
	@ResponseBody
	public String showCollections() {
		Set<String> results = new HashSet<String>();
		Set<String> collections = this.mongoTemplate.getCollectionNames();
		for (String collection : collections) {
			if (!collection.contains("system")) {
				results.add(collection);
			}
		}
		return JSON.serialize(results);
	}

	@RequestMapping(value = "/{collection}", method = RequestMethod.GET)
	@ResponseBody
	public String list(@PathVariable String collection) {
		List<DBObject> results = this.mongoTemplate.findAll(DBObject.class, collection);
		for (DBObject result : results) {
			result.removeField("_class");
			result.removeField("_id");
		}
		return JSON.serialize(results);
	}

	@RequestMapping(value = "/{collection}", method = RequestMethod.POST)
	@ResponseStatus(HttpStatus.CREATED)
	public void post(@RequestBody String jsonString, @PathVariable String collection) {
		Object o = JSON.parse(jsonString);
		if (o instanceof DBObject) {
			this.counters.putIfAbsent(collection, new AtomicLong());
			((DBObject) o).put("uid", this.counters.get(collection).incrementAndGet());
		}
		this.mongoTemplate.insert(o, collection);
	}

	@RequestMapping(value = "/{collection}/{uid}", method = RequestMethod.GET)
	@ResponseBody
	public String get(@PathVariable String collection, @PathVariable long uid) {
		Query query = new Query(Criteria.where("uid").is(uid));
		List<DBObject> results = this.mongoTemplate.find(query, DBObject.class, collection);
		if (CollectionUtils.isEmpty(results)) {
			return null; // 404?
		}
		DBObject result = results.get(0);
		result.removeField("_class");
		result.removeField("_id");
		return JSON.serialize(result);
	}

	@RequestMapping("/env")
	public void env(HttpServletResponse response) throws IOException {
		response.setContentType("text/plain");
		PrintWriter out = response.getWriter();
		out.println("System Properties:");
		for (Map.Entry<Object, Object> property : System.getProperties().entrySet()) {
			out.println(property.getKey() + ": " + property.getValue());
		}
		out.println();
		out.println("System Environment:");
		for (Map.Entry<String, String> envvar : System.getenv().entrySet()) {
			out.println(envvar.getKey() + ": " + envvar.getValue());
		}
	}

}
