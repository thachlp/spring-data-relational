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

import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticAssertions.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticJoinPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticViewPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.ForeignKeyPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.MaxPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.TableDefinitionPattern.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class AnalyticStructureBuilderTests {

	private static Object extractColumn(Object c) {

		if (c instanceof AnalyticStructureBuilder.BaseColumn bc) {
			return bc.getColumn();
		} else if (c instanceof AnalyticStructureBuilder.DerivedColumn dc) {
			return extractColumn(dc.getBase());
		} else if (c instanceof AnalyticStructureBuilder.RowNumber rn) {
			return "RN";
		} else if (c instanceof AnalyticStructureBuilder.ForeignKey fk) {
			return "FK(" + fk.getColumn() + ")";
		} else if (c instanceof AnalyticStructureBuilder.Max max) {
			return "MAX(" + extractColumn(max.getLeft()) + ", " + extractColumn(max.getRight()) + ")";
		} else {
			return "unknown";
		}
	}

	/**
	 * A simple table should result in a simple select. Columns are represented by
	 * {@link org.springframework.data.jdbc.core.convert.sqlgeneration.AnalyticStructureBuilder.BaseColumn} since they are
	 * directly referenced.
	 */
	@Test
	void simpleTableWithColumns() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>().addTable("person",
				td -> td.withId("person_id").withColumns("name", "lastname"));

		assertThat(builder).hasExactColumns("person_id", "name", "lastname") //
				.hasId("person_id") //
				.hasStructure(td("person"));

	}

	@Test
	void tableWithSingleChild() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parent-name", "parent-lastname"))
				.addChildTo("parent", "child", td -> td.withColumns("child-name", "child-lastname"));

		assertThat(builder).hasExactColumns( //
				"parentId", "parent-name", "parent-lastname", //
				"child-name", "child-lastname", //
				fk("child", "parentId"), max("parentId", fk("child", "parentId")) //
		).hasId("parentId") //
				.hasStructure(aj(td("parent"), av(td("child"))));
	}

	@Test
	void tableWithSingleChildWithKey() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parentName", "parentLastname"))
				.addChildTo("parent", "child", td -> td.withColumns("childName", "childLastname").withKeyColumn("childKey"));

		assertThat(builder) //
				.hasExactColumns("parentId", "parentName", "parentLastname", //
						max("parentId", //
								fk("child", "parentId")),
						fk("child", "parentId"), //
						"childKey", "childName", "childLastname") //
				.hasId("parentId") //
				.hasStructure(aj(td("parent"), av(td("child"))));
	}

	@Test
	void tableWithMultipleChildren() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parentName", "parentLastname"))
				.addChildTo("parent", "child1", td -> td.withColumns("childName", "childLastname"))
				.addChildTo("parent", "child2", td -> td.withColumns("siblingName", "siblingLastName"));

		AnalyticStructureBuilder<String, String>.Select select = builder.getSelect();

		assertThat(builder) //
				.hasExactColumns("parentId", "parentName", "parentLastname", //
						max("parentId", fk("child1", "parentId")), //
						fk("child1", "parentId"), //
						"childName", "childLastname", //
						max("parentId", fk("child2", "parentId")), //
						fk("child2", "parentId"), //
						"siblingName", //
						"siblingLastName")
				.hasId("parentId") //
				.hasStructure(aj(aj(td("parent"), av(td("child1"))), av(td("child2"))));
	}

	@Nested
	@Disabled // these tests are currently only sketches. the asserts need to be reviewed, and the necessary code for them
						// implemented.
	class TableWithChainOfChildren {

		@Test
		void middleChildHasId() {

			AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent", td -> td.withId("parentId").withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName"));

			AnalyticStructureBuilder<String, String>.Select select = builder.getSelect();

			assertThat(select.getColumns()).extracting(AnalyticStructureBuilderTests::extractColumn)
					.containsExactlyInAnyOrder("grannyId", "grannyName", max("grannyId", fk("parent","grannyId")), fk("parent","grannyId"),
							"parentId", "parentName", max("parentId", fk("child","parentId")), fk("child","parentId"), "childName");
			assertThat(select.getId()).extracting(c -> c.getColumn()).isEqualTo(0);

			assertThat(select.toString()).isEqualTo("AJ {p=TD{granny}, c=AJ {p=TD{parent}, c=AV{TD{child}}}}");
		}

		/**
		 * middle children that don't have an id, nor a key can't referenced by further children
		 */
		@Test
		void middleChildHasNoId() {

			AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent", td -> td.withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName"));

			assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> builder.getSelect());
		}

		@Test
		void middleChildWithKeyHasId() {

			AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent",
							td -> td.withId("parentId").withKeyColumn("parentKey").withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName"));

			AnalyticStructureBuilder<String, String>.Select select = builder.getSelect();

			assertThat(builder).hasExactColumns( //
					"grannyId", "grannyName", //
					max("grannyId", fk("parent","grannyId")), //
					fk("parent","grannyId"), //
					"parentKey", "parentName", //
					max("parentId", fk("child","parentId")), //
					fk("child","parentId"), //
					"childName" //
			).hasId("grannyId");

			assertThat(select.toString()).isEqualTo("AJ {p=TD{granny}, c=AJ {p=TD{parent}, c=AV{TD{child}}}}");
		}

		@Test
		void middleChildWithKeyHasNoId() {

			AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addChildTo("granny", "parent", td -> td.withColumns("parentName").withKeyColumn("parentKey"))
					.addChildTo("parent", "child", td -> td.withColumns("childName"));

			AnalyticStructureBuilder<String, String>.Select select = builder.getSelect();

			assertThat(builder).hasExactColumns( //
					"grannyId", "grannyName", //
					max("grannyId", fk("parent","grannyId")), //
					fk("parent","grannyId"), //
					"parentKey", //
					"parentName", //
					max(fk("parent","grannyId"), fk("???",fk("parent","grannyId"))), //
					fk("???",fk("parent", "grannyId")), //
					max("parentKey", fk("child","parentKey")), //
					fk("child","parentKey"), //
					"childName" //
			).hasId("grannyId");

			assertThat(select.toString()).isEqualTo("AJ {p=TD{granny}, c=AJ {p=TD{parent}, c=AV{TD{child}}}}");
		}

		@Test
		void middleSingleChildHasId() {

			AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addSingleChildTo("granny", "parent", td -> td.withId("parentId").withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName"));

			AnalyticStructureBuilder<String, String>.Select select = builder.getSelect();

			assertThat(builder).hasExactColumns( //
					"grannyId", "grannyName", //
					fk("parent","grannyId"), //
					"parentId", "parentName", //
					fk("???", fk("parent","grannyId")), "childName" //
			).hasId("grannyId");

			assertThat(select.toString()).isEqualTo("AJ {p=TD{granny}, c=AJ {p=TD{parent}, c=AV{TD{child}}}}");
		}

		@Test
		void middleSingleChildHasNoId() {

			AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
					.addTable("granny", td -> td.withId("grannyId").withColumns("grannyName"))
					.addSingleChildTo("granny", "parent", td -> td.withColumns("parentName"))
					.addChildTo("parent", "child", td -> td.withColumns("childName"));

			AnalyticStructureBuilder.Select select = builder.getSelect();

			assertThat(builder).hasExactColumns("grannyId", "grannyName", fk("parent","grannyId"), "parentName", "childName"); // TODO:
																																																								// Max
																																																								// column?
			assertThat(select.getId()).extracting(c -> c.getColumn()).isEqualTo("grannyId");

			assertThat(select.toString()).isEqualTo("AJ {p=TD{granny}, c=AJ {p=TD{parent}, c=AV{TD{child}}}}");
		}
	}

	@Test
	void mediumComplexHierarchy() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName"));
		builder.addChildTo("customer", "address", td -> td.withId("addressId").withColumns("addressName"));
		builder.addChildTo("address", "city", td -> td.withColumns("cityName"));
		builder.addChildTo("customer", "order", td -> td.withId("orderId").withColumns("orderName"));
		builder.addChildTo("address", "type", td -> td.withColumns("typeName"));

		assertThat(builder)
				.hasExactColumns( //
						"customerId", "customerName", //
						max("customerId",fk("address", "customerId")),fk("address", "customerId"), "addressId", "addressName", // <--
						max("addressId", fk("city", "addressId")), fk("city", "addressId"), "cityName", //
						max("customerId",fk("order", "customerId")),fk("order", "customerId"),"orderId", "orderName", // <--
						max("addressId", fk("type", "addressId")), fk("type", "addressId"), "typeName"//
				).hasId("customerId") //
				.hasStructure( //
						aj( //
								aj( //
										td("customer"), //
										aj( //
												aj( //
														td("address"), //
														av(td("city")) //
												), //
												av(td("type")) //
										) //
								), //
								av(td("order")) //
						) //
				);

	}

	@Test
	void mediumComplexHierarchy2() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName"));
		builder.addChildTo("customer", "keyAccount", td -> td.withId("keyAccountId").withColumns("keyAccountName"));
		builder.addChildTo("keyAccount", "assistant", td -> td.withColumns("assistantName"));
		builder.addChildTo("customer", "order", td -> td.withId("orderId").withColumns("orderName"));
		builder.addChildTo("keyAccount", "office", td -> td.withColumns("officeName"));
		builder.addChildTo("order", "item", td -> td.withId("itemId").withColumns("itemName"));

		assertThat(builder).hasStructure( //
				aj( //
						aj( //
								td("customer"), //
								aj( //
										aj( //
												td("keyAccount"), //
												av(td("assistant")) //
										), //
										av(td("office")) //
								) //
						), //
						aj( //
								td("order"), //
								av(td("item")) //
						) //
				) //
		);

	}

	@Test
	void complexHierarchy() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("customer", td -> td.withId("customerId").withColumns("customerName"));
		builder.addChildTo("customer", "keyAccount", td -> td.withId("keyAccountId").withColumns("keyAccountName"));
		builder.addChildTo("keyAccount", "assistant", td -> td.withColumns("assistantName"));
		builder.addChildTo("customer", "order", td -> td.withId("orderId").withColumns("orderName"));
		builder.addChildTo("keyAccount", "office", td -> td.withId("officeId").withColumns("officeName"));
		builder.addChildTo("order", "item", td -> td.withColumns("itemName"));
		builder.addChildTo("order", "shipment", td -> td.withColumns("shipmentName"));
		builder.addChildTo("office", "room", td -> td.withColumns("roomNumber"));

		AnalyticStructureBuilder.Select select = builder.getSelect();

		assertThat(builder).hasExactColumns( //
				"customerId", "customerName", // TODO: maxes are missing
				max("customerId",fk("keyAccount", "customerId")),fk("keyAccount", "customerId"), "keyAccountId", "keyAccountName", //
				max("keyAccountId",fk("office", "keyAccountId")), "officeName", //
				max("customerId", fk("order", "customerId")), "orderId", "orderName", //
				max("keyAccountId", fk("assistant", "keyAccountId")), fk("assistant", "keyAccountId"), "assistantName", //
				max("orderId", fk("item", "orderId")), fk("item", "orderId"), "itemName", //
				max("orderId", fk("shipment", "orderId")), fk("shipment", "orderId"), "shipmentName", "officeId", //
				max("officeId", fk("room", "officeId")), fk("room", "officeId"), "roomNumber" //
		).hasId("customerId") //
				.hasStructure( //
						aj( //
								aj( //
										td("customer"), //
										aj( //
												aj( //
														td("keyAccount"), //
														av(td("assistant")) //
												), //
												aj( //
														td("office"), //
														av(td("room")) //
												) //
										) //
								), //
								aj( //
										aj( //
												td("order"), //
												av(td("item")) //
										), //
										av(td("shipment")) //
								) //
						) //
				);

	}

	// TODO: Joins must contain the fields to join on:
	// Max of parent key and fk for all pairs
	// - key values of parents
	// - rownumber

	private Set<AnalyticStructureBuilder<String, Integer>.TableDefinition> collectFroms(
			AnalyticStructureBuilder<String, Integer>.Select select) {

		Set<AnalyticStructureBuilder<String, Integer>.TableDefinition> froms = new HashSet<>();
		select.getFroms().forEach(s -> {
			if (s instanceof AnalyticStructureBuilder.AnalyticJoin) {
				froms.addAll(collectFroms(s));
			} else if (s instanceof AnalyticStructureBuilder.AnalyticView) {

				froms.add((AnalyticStructureBuilder.TableDefinition) s.getFroms().get(0));
			} else {
				froms.add((AnalyticStructureBuilder.TableDefinition) s);
			}
		});

		return froms;
	}

}
