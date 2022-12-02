/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.core.convert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;

class CachingResultSet {

	private static final Log log = LogFactory.getLog(CachingResultSet.class);

	private final ResultSet delegate;
	private Cache cache;

	CachingResultSet(ResultSet delegate) {

		this.delegate = delegate;
	}

	public boolean next() {

		if (isPeeking()) {

			final boolean next = cache.next;
			cache = null;
			return next;
		}

		try {
			return delegate.next();
		} catch (SQLException e) {
			throw new RuntimeException("Failed to advance CachingResultSet", e);
		}
	}

	@Nullable
	public Object getObject(String columnLabel) {

		Object returnValue;
		if (isPeeking()) {
			returnValue = cache.values.get(columnLabel);
		} else {
			returnValue = safeGetFromDelegate(columnLabel);
		}

		System.out.println(" returning " + returnValue);
		return returnValue;
	}

	@Nullable
	Object peek(String columnLabel) {

		if (!isPeeking()) {
			createCache();
		}

		if (!cache.next) {
			return null;
		}

		return safeGetFromDelegate(columnLabel);
	}

	@Nullable
	private Object safeGetFromDelegate(String columnLabel) {
		try {
			return delegate.getObject(columnLabel);
		} catch (SQLException e) {
			if (log.isDebugEnabled()) {
				log.debug("Failed to access column '" + columnLabel + "': " + e.getMessage());
			}

			return null;
		}
	}

	private void createCache() {
		cache = new Cache();

		try {
			int columnCount = delegate.getMetaData().getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				// at least some databases return lower case labels although rs.getObject(UPPERCASE_LABEL) returns the expected
				// value. The aliases we use happen to be uppercase. So we transform everything to upper case.
				cache.add(delegate.getMetaData().getColumnLabel(i).toUpperCase(), delegate.getObject(i));
			}

			cache.next = delegate.next();
		} catch (SQLException se) {
			throw new RuntimeException("Can't cache result set data", se);
		}

	}

	private boolean isPeeking() {
		return cache != null;
	}

	private static class Cache {

		boolean next;
		Map<String, Object> values = new HashMap<>();

		void add(String columnName, Object value) {
			values.put(columnName, value);
		}
	}
}
