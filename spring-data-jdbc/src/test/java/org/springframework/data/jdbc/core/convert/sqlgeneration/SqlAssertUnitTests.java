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

package org.springframework.data.jdbc.core.convert.sqlgeneration;

import static org.springframework.data.jdbc.core.convert.sqlgeneration.SqlAssert.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlAssertUnitTests {

	@Test
	void acceptsSimpleSelect() {
		assertThatParsed("select a, b from table") //
				.selectsFrom("table");
	}

	@Test
	void notesWrongTableName() {

		Assertions.assertThrows( //
				AssertionError.class, //
				() -> assertThatParsed("select a from table") //
						.selectsFrom("wrong table") //
		);
	}

	@Test
	void findWhereClause() {
		assertThatParsed("select * from x where 1 = 2").hasWhereClause();
	}

	@Test
	void findWhereClauseNegative() {
		assertThatParsed("select * from x").hasNoWhereClause();
	}

	@Test
	void findOriginalSelectAsSubselect() {
		assertThatParsed("select * from x where 1 = 2").hasSubselectFrom("x").hasWhereClause();
	}
	@Test
	void findSubSelectInFrom() {
		assertThatParsed("select * from (select * from x where 1 = 2) y").hasSubselectFrom("x").hasWhereClause();
	}
	@Test
	void findSubSelectInJoin() {
		assertThatParsed("select * from z join (select * from x where 1 = 2) y").hasSubselectFrom("x").hasWhereClause();
	}
}
