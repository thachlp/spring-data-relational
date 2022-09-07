package org.springframework.data.jdbc.core.convert.sqlgeneration;
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

public record AnalyticJoinPattern(StructurePattern left, StructurePattern right) implements StructurePattern {

	static AnalyticJoinPattern aj(StructurePattern left, StructurePattern right) {
		return new AnalyticJoinPattern(left, right);
	}

	@Override
	public boolean matches(AnalyticStructureBuilder<?, ?>.Select select) {

		if (select instanceof AnalyticStructureBuilder.AnalyticJoin join) {
			return left().matches(join.getParent()) && right().matches(join.getChild());
		}
		return false;
	}

	@Override
	public String toString() {
		return "AJ{" + left + ", " + right + '}';
	}
}