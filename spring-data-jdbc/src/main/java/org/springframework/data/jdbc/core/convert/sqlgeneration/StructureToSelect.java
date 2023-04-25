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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.*;
import org.springframework.util.Assert;

public class StructureToSelect {

	public static final BiConsumer<PersistentPropertyPathExtension, Expression> NOOP_ID_REGISTRATION = (path,
			expression) -> {};
	private final AliasFactory aliasFactory;

	StructureToSelect() {
		this(new AliasFactory());
	}

	public StructureToSelect(AliasFactory aliasFactory) {
		this.aliasFactory = aliasFactory;
	}

	SelectConstruction createSelect(
			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select queryStructure,
			Function<TableLike, Condition> condition) {

		return new SelectConstructionContext(queryStructure, condition).createSelect();
	}

	class SelectConstruction {
		SelectBuilder.BuildSelect select;
		final AnalyticStructureBuilder.Select queryStructure;
		private Map<PersistentPropertyPathExtension, Expression> idExpressions;

		public SelectConstruction(SelectBuilder.BuildSelect select, AnalyticStructureBuilder.Select queryStructure) {

			this.select = select;
			this.queryStructure = queryStructure;
		}

		public Select findAll() {
			return select.build();
		}

		public Select findById() {

			return select.build();
		}

		public Select findAllById() {
			return select.build();
		}

		private Expression getIdColumn(List<AnalyticStructureBuilder.AnalyticColumn> ids) {

			Expression expression = idExpressions.get(ids.get(0).getColumn());

			Assert.state(expression != null, "id column must not be null");

			return expression;
		}

		private SelectConstruction orderBy(Collection<OrderByField> orderByFields) {
			select = ((SelectBuilder.SelectOrdered) select).orderBy(orderByFields);
			return this;
		}

		public void setIdMap(Map<PersistentPropertyPathExtension, Expression> idExpressions) {

			this.idExpressions = idExpressions;
		}

	}

	private class SelectConstructionContext {
		private final AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select queryStructure;
		private final Function<TableLike, Condition> condition;

		public SelectConstructionContext(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select queryStructure,
				Function<TableLike, Condition> condition) {

			this.queryStructure = queryStructure;
			this.condition = condition;
		}

		public SelectConstruction createSelect() {
			return createSelect(queryStructure);
		}

		SelectConstruction createSelect(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select queryStructure) {

			Map<PersistentPropertyPathExtension, Expression> idExpressions = new HashMap<>();

			BiConsumer<PersistentPropertyPathExtension, Expression> registerIdExpression = (path, expression) -> {
				idExpressions.put(path, expression);
			};

			SelectConstruction selectConstruction = createUnorderedSelect(queryStructure, registerIdExpression);

			if (queryStructure instanceof AnalyticStructureBuilder.AnalyticJoin join) {

				Collection<OrderByField> orderByFields = new ArrayList<>();
				join.getId().forEach(id -> {
					orderByFields.add(OrderByField.from(createAliasExpression((AnalyticStructureBuilder.DerivedColumn) id)));
				});

				orderByFields.add(OrderByField.from(createAliasExpression(join.getRowNumber())));
				selectConstruction.orderBy(orderByFields);
			}
			selectConstruction.setIdMap(idExpressions);
			return selectConstruction;
		}

		private SelectConstruction createUnorderedSelect(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select queryStructure,
				BiConsumer<PersistentPropertyPathExtension, Expression> registerIdExpression) {

			SelectBuilder.BuildSelect select;

			if (queryStructure instanceof AnalyticStructureBuilder.TableDefinition tableDefinition) {
				select = createSimpleSelect(tableDefinition, registerIdExpression);
			} else if (queryStructure instanceof AnalyticStructureBuilder.AnalyticJoin analyticJoin) {
				select = createJoin(analyticJoin, registerIdExpression);
			} else if (queryStructure instanceof AnalyticStructureBuilder.AnalyticView analyticView) {
				select = createView(analyticView, registerIdExpression);
			} else {
				throw new UnsupportedOperationException("Can't convert " + queryStructure);
			}

			return new SelectConstruction(select, queryStructure);

		}

		private SelectBuilder.BuildSelect createView(AnalyticStructureBuilder.AnalyticView analyticView,
				BiConsumer<PersistentPropertyPathExtension, Expression> registerIdExpression) {
			return createSimpleSelect(analyticView, registerIdExpression);
		}

		private SelectBuilder.SelectOrdered createJoin(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticJoin analyticJoin,
				BiConsumer<PersistentPropertyPathExtension, Expression> registerIdExpression) {

			AnalyticStructureBuilder.Select child = analyticJoin.getChild();
			SelectConstruction childSelect = createUnorderedSelect(child, NOOP_ID_REGISTRATION);
			InlineQuery childQuery = InlineQuery.create(childSelect.findAll(), getAliasFor(child));

			AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select parent = analyticJoin
					.getParent();
			TableLike parentTable = tableFor(parent);
			Collection<Expression> columns = createSelectExpressionList(analyticJoin.getColumns(), parentTable,
					registerIdExpression);

			SelectBuilder.SelectFromAndJoin selectAndParent = StatementBuilder.select(columns).from(parentTable);

			Condition joinCondition = createJoinCondition(parentTable, childQuery, analyticJoin);
			SelectBuilder.SelectFromAndJoinCondition joinThingy = selectAndParent
					.join(childQuery, Join.JoinType.FULL_OUTER_JOIN).on(joinCondition);

			if (condition != null
					&& parent instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition td
					&& td.getTable().equals(queryStructure.getRoot())) {

				System.out.println("applying condition on join");
				return joinThingy.where(condition.apply(parentTable));
			}

			return joinThingy;
		}

		private String getAliasFor(Object object) {

			String alias = aliasFactory.getOrCreateAlias(object);

			if (object instanceof AnalyticStructureBuilder.RowNumber rowNumber) {
				Object firstPartitionBy = rowNumber.getPartitionBy().get(0);
				if (firstPartitionBy instanceof AnalyticStructureBuilder.ForeignKey foreignKey) {
					RelationalPersistentEntity table = ((AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.ForeignKey) foreignKey)
							.getOwner().getTable();
					aliasFactory.put(table, alias);
				} else {
					throw new IllegalStateException("we can only handle foreign keys in this position" + firstPartitionBy);
				}
			}

			return alias;
		}

		// TODO: table is not required when the column is derived
		private Expression createColumn(TableLike table,
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn analyticColumn,
				BiConsumer<PersistentPropertyPathExtension, Expression> registerIdExpression) {

			if (analyticColumn instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.ForeignKey foreignKey) {

				SqlIdentifier columnName = createFkColumnName(foreignKey);
				String alias = getAliasFor(foreignKey);
				return table.column(columnName).as(alias);
			}
			if (analyticColumn instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.KeyColumn keyColumn) {

				SqlIdentifier columnName = createKeyColumnName(keyColumn);
				String alias = getAliasFor(keyColumn);
				return table.column(columnName).as(alias);
			}
			if (analyticColumn instanceof AnalyticStructureBuilder.DerivedColumn derivedColumn) {
				return createAliasExpression(derivedColumn);
			}
			if (analyticColumn instanceof AnalyticStructureBuilder.Literal al) {
				return SQL.literalOf(al.getValue());
			}
			PersistentPropertyPathExtension property = analyticColumn.getColumn();
			if (property != null) {
				Column column = createSimpleColumn(table, property);
				registerIdExpression.accept(analyticColumn.getColumn(), column);
				return column;
			}
			if (analyticColumn instanceof AnalyticStructureBuilder.RowNumber rn) {

				return createRownumberExpression(table, rn);
			}
			if (analyticColumn instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Greatest gt) {

				Expression leftColumn = createColumn(table, gt.left, NOOP_ID_REGISTRATION);
				Expression rightColumn = createColumn(table, gt.right, NOOP_ID_REGISTRATION);

				SimpleFunction column = SimpleFunction.create("COALESCE", Arrays.asList(leftColumn, rightColumn))
						.as(getAliasFor(gt));
				PersistentPropertyPathExtension path = gt.left.getColumn();
				if (path != null) {
					registerIdExpression.accept(path, column);
				}
				return column;
			} else {
				throw new UnsupportedOperationException("Can't handle " + analyticColumn);
			}

		}

		private Expression createAliasExpression(AnalyticStructureBuilder.AnalyticColumn column) {
			return Expressions.just(aliasFactory
					.getOrCreateAlias(column instanceof AnalyticStructureBuilder.DerivedColumn dc ? dc.getBase() : column));
		}

		private Expression createRownumberExpression(TableLike parentTable,
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.RowNumber rn) {
			Expression column;
			Expression[] partitionBys = (rn.getPartitionBy().stream()
					.map(ac -> createColumn(parentTable, ac, NOOP_ID_REGISTRATION))).toArray(Expression[]::new);
			Expression[] orderBys = (rn.getOrderBy().stream().map(ac -> createColumn(parentTable, ac, NOOP_ID_REGISTRATION)))
					.toArray(Expression[]::new);

			column = AnalyticFunction.create("ROW_NUMBER") //
					.partitionBy(partitionBys) //
					.orderBy(orderBys) //
					.as(getAliasFor(rn));
			return column;
		}

		private Column createSimpleColumn(TableLike parentTable, PersistentPropertyPathExtension path) {

			String alias = getAliasFor(path);

			return parentTable //
					.column(path.getColumnName()) //
					.as(alias);
		}

		private TableLike tableFor(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select parent) {

			if (parent instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition tableDefinition) {
				return Table.create(tableDefinition.getTable().getQualifiedTableName()).as(getAliasFor(parent));
			}

			if (parent instanceof AnalyticStructureBuilder.AnalyticView av) {
				return tableFor((AnalyticStructureBuilder.TableDefinition) av.getParent());
			}

			throw new UnsupportedOperationException("can only create table names for TableDefinitions right now");
		}

		private Condition createJoinCondition(TableLike parentTable, TableLike childQuery,
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticJoin analyticJoin) {

			Condition condition = null;
			for (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.JoinCondition joinCondition : analyticJoin
					.getConditions()) {

				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn left = joinCondition
						.getLeft();
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn right = joinCondition
						.getRight();

				Comparison newCondition = Conditions.isEqual(createColumn(parentTable, left, NOOP_ID_REGISTRATION),
						createColumn(childQuery, right, NOOP_ID_REGISTRATION));
				if (condition == null) {
					condition = newCondition;
				} else {
					condition = condition.and(newCondition);
				}
			}
			return condition;
		}

		private SelectBuilder.SelectOrdered createSimpleSelect(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.Select select,
				BiConsumer<PersistentPropertyPathExtension, Expression> registerIdExpression) {

			List<? extends AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn> analyticColumns = select
					.getColumns();

			TableLike table = tableFor(select);

			Collection<Expression> selectExpressionList = createSelectExpressionList(analyticColumns, table,
					registerIdExpression);

			SelectBuilder.SelectFromAndJoin from = StatementBuilder.select(selectExpressionList).from(table);

			if (condition != null
					&& select instanceof AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.TableDefinition td
					&& td.getTable().equals(queryStructure.getRoot())) {

				return from.where(condition.apply(table));
			}
			return from;
		}

		private Collection<Expression> createSelectExpressionList(
				List<? extends AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn> analyticColumns,
				TableLike table, BiConsumer<PersistentPropertyPathExtension, Expression> registerIdExpression) {

			Collection<Expression> selectExpressionList = new ArrayList<>();

			for (AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.AnalyticColumn analyticColumn : analyticColumns) {
				Expression column = createColumn(table, analyticColumn, registerIdExpression);
				selectExpressionList.add(column);
			}
			return selectExpressionList;
		}

		private static SqlIdentifier createFkColumnName(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.ForeignKey foreignKey) {

			return foreignKey.getOwner().getPathInformation().getReverseColumnName();
		}

		private static SqlIdentifier createKeyColumnName(
				AnalyticStructureBuilder<RelationalPersistentEntity, PersistentPropertyPathExtension>.KeyColumn keyColumn) {

			return keyColumn.getColumn().getQualifierColumn();
		}
	}
}
