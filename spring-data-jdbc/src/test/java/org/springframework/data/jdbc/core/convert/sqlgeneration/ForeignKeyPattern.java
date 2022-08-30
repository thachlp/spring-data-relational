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
package org.springframework.data.jdbc.core.convert.sqlgeneration;

import org.springframework.lang.Nullable;

record ForeignKeyPattern<C>(C name) implements Pattern{

	public static <C> ForeignKeyPattern<C> fk(String name){
		return new ForeignKeyPattern(name);
	}

	@Override
	public boolean matches(AnalyticStructureBuilder.AnalyticColumn other) {
		return extractForeignKey(other) != null && name.equals(other.getColumn());
	}

	@Nullable
	static AnalyticStructureBuilder.ForeignKey extractForeignKey(AnalyticStructureBuilder.AnalyticColumn column) {
		if (column instanceof AnalyticStructureBuilder.ForeignKey foreignKey) {
			return foreignKey;
		}

		if (column instanceof AnalyticStructureBuilder.DerivedColumn derivedColumn) {
			return extractForeignKey(derivedColumn.getBase());
		}
		return null;
	}
}
