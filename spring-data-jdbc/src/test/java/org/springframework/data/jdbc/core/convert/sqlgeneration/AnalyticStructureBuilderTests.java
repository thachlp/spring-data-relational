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
import static org.springframework.data.jdbc.core.convert.sqlgeneration.ForeignKeyPattern.*;
import static org.springframework.data.jdbc.core.convert.sqlgeneration.MaxPattern.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.lang.Nullable;

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
				.hasId("person_id");

	}

	@Test
	void tableWithSingleChild() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parent-name", "parent-lastname"))
				.addChildTo("parent", "child", td -> td.withColumns("child-name", "child-lastname"));

		assertThat(builder).hasExactColumns("parentId", "parent-name", "parent-lastname", //
				"child-name", "child-lastname", //
				fk("parentId"), max("parentId", fk("parentId"))) //
				.hasId("parentId");

		AnalyticStructureBuilder<String, String>.Select select = builder.getSelect();
		assertThat(stringify(select)).containsExactlyInAnyOrder( //
				"AJ -> TD(parent)", //
				"AJ -> AV -> TD(child)");
	}

	@Test
	void tableWithSingleChildWithKey() {

		AnalyticStructureBuilder<String, String> builder = new AnalyticStructureBuilder<String, String>()
				.addTable("parent", td -> td.withId("parentId").withColumns("parentName", "parentLastname"))
				.addChildTo("parent", "child", td -> td.withColumns("childName", "childLastname").withKeyColumn("childKey"));

		assertThat(builder) //
				.hasExactColumns("parentId", "parentName", "parentLastname", //
						max("parentId", //
								fk("parentId")),
						fk("parentId"), //
						"childKey", "childName", "childLastname") //
				.hasId("parentId");

		assertThat(stringify(builder.getSelect())).containsExactlyInAnyOrder( //
				"AJ -> TD(parent)", //
				"AJ -> AV -> TD(child)");

		assertThatSelect(builder.getSelect()).joins("parent", "child").on("parentId");
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
						max("parentId", fk("parentId")), //
						fk("parentId"), //
						"childName", "childLastname", //
						max("parentId", fk("parentId")), //
						fk("parentId"), //
						"siblingName", //
						"siblingLastName")
				.hasId("parentId");

		assertThat(stringify(select)).containsExactlyInAnyOrder( //
				"AJ -> AJ -> TD(parent)", //
				"AJ -> AJ -> AV -> TD(child1)", //
				"AJ -> AV -> TD(child2)");

	}

	@Nested
	@Disabled // these tests are currently only scetches. the asserts need to be reviewed, and the necessary code for them
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
					.containsExactlyInAnyOrder("grannyId", "grannyName", max("grannyId", fk("grannyId")), fk("grannyId"),
							"parentId", "parentName", max("parentId", fk("parentId")), fk("parentId"), "childName");
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
					max("grannyId", fk("grannyId")), //
					fk("grannyId"), //
					"parentKey", "parentName", //
					max("parentId", fk("parentId")), //
					fk("parentId"), //
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
					max("grannyId", fk("grannyId")), //
					fk("grannyId"), //
					"parentKey", //
					"parentName", //
					max(fk("grannyId"), fk(fk("grannyId"))), //
					fk(fk("grannyId")), //
					max("parentKey", fk("parentKey")), //
					fk("parentKey"), //
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
					fk("grannyId"), //
					"parentId", "parentName", //
					fk(fk("grannyId")), "childName" //
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

			assertThat(builder).hasExactColumns("grannyId", "grannyName", fk("grannyId"), "parentName", "childName"); // TODO:
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

		AnalyticStructureBuilder.Select select = builder.getSelect();

		assertThat(builder).hasExactColumns("customerId", "customerName", fk("customerId"), "addressId", "addressName",
				max("addressId", fk("addressId")), fk("addressId"), "cityName", fk("customerId"), "orderId", "orderName",
				max("addressId", fk("addressId")), fk("addressId"), "typeName"// TODO: Why no max columns for some FK here?
		).hasId("customerId");

		assertThat(select.toString()).isEqualTo(
				"AJ {p=AJ {p=TD{customer}, c=AJ {p=AJ {p=TD{address}, c=AV{TD{city}}}, c=AV{TD{type}}}}, c=AV{TD{order}}}");

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

		assertThat(builder.getSelect().toString()).isEqualTo(
				"AJ {p=AJ {p=TD{customer}, c=AJ {p=AJ {p=TD{keyAccount}, c=AV{TD{assistant}}}, c=AV{TD{office}}}}, c=AJ {p=TD{order}, c=AV{TD{item}}}}");

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
				"customerId", "customerName", //
				fk("customerId"), "keyAccountId", "keyAccountName", //
				fk("keyAccountId"), "assistantName", //
				fk("customerId"), "orderId", "orderName", //
				max("keyAccountId", fk("keyAccountId")), fk("keyAccountId"), "officeName", //
				max("orderId", fk("orderId")), fk("orderId"), "itemName", //
				max("orderId", fk("orderId")), fk("orderId"), "shipmentName", "officeId", //
				max("officeId", fk("officeId")), fk("officeId"), "roomNumber" //
		).hasId("customerId");

		assertThat(select.toString()).isEqualTo(
				"AJ {p=AJ {p=TD{customer}, c=AJ {p=AJ {p=TD{keyAccount}, c=AV{TD{assistant}}}, c=AJ {p=TD{office}, c=AV{TD{room}}}}}, c=AJ {p=AJ {p=TD{order}, c=AV{TD{item}}}, c=AV{TD{shipment}}}}");

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

	private List<String> stringify(AnalyticStructureBuilder<String, ?>.Select select) {

		if (select instanceof AnalyticStructureBuilder.TableDefinition) {
			return Collections
					.singletonList("TD(%s)".formatted(((AnalyticStructureBuilder.TableDefinition) select).getTable()));
		} else {

			String prefix = select instanceof AnalyticStructureBuilder.AnalyticView ? "AV" : "AJ";
			return select.getFroms().stream().flatMap(f -> stringify(f).stream()).map(s -> prefix + " -> " + s).toList();
		}
	}

	private static SelectAssert assertThatSelect(AnalyticStructureBuilder.Select select) {
		return new SelectAssert(select);
	}

	private static class SelectAssert {
		private final AnalyticStructureBuilder.Select select;

		public SelectAssert(AnalyticStructureBuilder.Select select) {

			this.select = select;
		}

		JoinAssert joins(String parent, String child) {

			AnalyticStructureBuilder.AnalyticJoin join = findJoin(select, parent, child);
			assertThat(join).describedAs("No Join found for %s and %s", parent, child).isNotNull();
			return new JoinAssert(join);
		}

		@Nullable
		private AnalyticStructureBuilder.AnalyticJoin findJoin(AnalyticStructureBuilder.Select select, String parent,
				String child) {

			if (!(select instanceof AnalyticStructureBuilder.AnalyticJoin join)) {
				return null;
			}

			if (parent.equals(unwrap(join.getParent())) && child.equals(unwrap(join.getChild()))) {
				return join;
			}

			AnalyticStructureBuilder.AnalyticJoin parentSearchResult = findJoin(join.getParent(), parent, child);
			if (parentSearchResult != null) {
				return parentSearchResult;
			}
			return findJoin(join.getChild(), parent, child);
		}

		private Object unwrap(AnalyticStructureBuilder.Select node) {

			if (node instanceof AnalyticStructureBuilder.AnalyticJoin) {
				return null;
			}

			if (node instanceof AnalyticStructureBuilder.TableDefinition td) {
				return td.getTable();
			}
			return unwrap((AnalyticStructureBuilder.Select) node.getParent());
		}
	}

	private static class JoinAssert {
		private final AnalyticStructureBuilder.AnalyticJoin join;

		public JoinAssert(AnalyticStructureBuilder.AnalyticJoin join) {

			this.join = join;
		}

		void on(Object idOfParent) {
			assertThat(join.getJoinCondition()).extracting(jc -> jc.getParent().getColumn()).isEqualTo(idOfParent);

		}
	}
}
