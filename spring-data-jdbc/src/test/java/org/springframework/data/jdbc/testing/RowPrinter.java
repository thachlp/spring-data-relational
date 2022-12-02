/*
 * Copyright 2023 the original author or authors.
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

package org.springframework.data.jdbc.testing;

import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class RowPrinter implements RowCallbackHandler {
	@Override
	public void processRow(ResultSet rs) throws SQLException {
		ResultSetMetaData metaData = rs.getMetaData();
		int columnCount = metaData.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			System.out.println(metaData.getColumnName(i) + "/" + metaData.getColumnLabel(i) + ": " + rs.getObject(i));
		}
		System.out.println();
	}
}
