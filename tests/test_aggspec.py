import unittest
from typing import Any

from query.aggspec import (
    AggFunc,
    AggregationBuilder,
    ValidationError,
    count,
    sum,
    avg,
    max,
    min,
    listagg,
    build_value_specs
)


class TestAggregationBuilder(unittest.TestCase):
    def test_simple_column_aggregation(self) -> None:
        """Test basic column aggregation with different functions"""
        spec = count.column("user_id").name("total_users").build()
        self.assertEqual(spec, {
            'aggfunc': 'COUNT',
            'column': 'user_id',
            'name': 'total_users',
            'distinct': False
        })

    def test_case_condition(self) -> None:
        """Test CASE condition with single condition"""
        spec = count.case("status = 'active'").name("active_users").build()
        self.assertEqual(spec, {
            'aggfunc': 'COUNT',
            'case': "WHEN status = 'active' THEN 1",
            'name': 'active_users',
            'distinct': False
        })

    def test_multiple_conditions(self) -> None:
        """Test CASE with multiple AND conditions"""
        conditions = ["status = 'active'", "type = 'premium'"]
        spec = count.case(conditions).name("premium_active").build()
        self.assertEqual(spec, {
            'aggfunc': 'COUNT',
            'case': "WHEN (status = 'active') AND (type = 'premium') THEN 1",
            'name': 'premium_active',
            'distinct': False
        })

    def test_multiple_conditions_or(self) -> None:
        """Test CASE with multiple OR conditions"""
        conditions = ["type = 'trial'", "type = 'premium'"]
        spec = count.case(conditions, operator='OR').name("paid_users").build()
        self.assertEqual(spec, {
            'aggfunc': 'COUNT',
            'case': "WHEN (type = 'trial') OR (type = 'premium') THEN 1",
            'name': 'paid_users',
            'distinct': False
        })

    def test_custom_then_clause(self) -> None:
        """Test CASE with custom THEN expression"""
        spec = (
            sum
            .case("is_valid = 1")
            .then("amount * weight")
            .name("weighted_sum")
            .build()
        )
        self.assertEqual(spec, {
            'aggfunc': 'SUM',
            'case': "WHEN is_valid = 1 THEN amount * weight",
            'name': 'weighted_sum',
            'distinct': False
        })

    def test_distinct_aggregation(self) -> None:
        """Test DISTINCT flag"""
        spec = count.column("category").distinct(True).name("unique_categories").build()
        self.assertEqual(spec, {
            'aggfunc': 'COUNT',
            'column': 'category',
            'name': 'unique_categories',
            'distinct': True
        })

    def test_getter_methods(self) -> None:
        """Test that methods work as getters when called without arguments"""
        agg = (
            count
            .name("test_name")
            .column("test_col")
            .distinct(True)
            .case("test_case")
            .then("test_then")
        )

        self.assertEqual(agg.name(), "test_name")
        self.assertEqual(agg.column(), "test_col")
        self.assertTrue(agg.distinct())
        self.assertEqual(agg.case(), "WHEN test_case THEN test_then")
        self.assertEqual(agg.then(), "test_then")

    def test_immutability(self) -> None:
        """Test that builder operations don't modify the original instance"""
        original = count.name("original")
        modified = original.name("modified")

        self.assertEqual(original.name(), "original")
        self.assertEqual(modified.name(), "modified")
        self.assertIsNot(original, modified)

    def test_preconfigured_builders(self) -> None:
        """Test that all pre-configured builders work correctly"""
        builders = [count, sum, avg, max, min, listagg]
        for builder in builders:
            with self.subTest(builder=builder):
                spec = builder.column("test").name("test_name").build()
                self.assertIsInstance(spec, dict)
                self.assertEqual(spec['aggfunc'], builder._spec['aggfunc'])

    def test_validation_missing_column_and_case(self) -> None:
        """Test validation fails when neither column nor case is specified"""
        with self.assertRaises(ValidationError) as cm:
            count.name("test").build()
        self.assertIn("Must specify either column or case", str(cm.exception))

    def test_validation_both_column_and_case(self) -> None:
        """Test validation fails when both column and case are specified"""
        with self.assertRaises(ValidationError) as cm:
            count.column("col").case("condition").name("test").build()
        self.assertIn("Cannot specify both column and case", str(cm.exception))

    def test_validation_case_without_name(self) -> None:
        """Test validation fails when case is used without name"""
        with self.assertRaises(ValidationError) as cm:
            count.case("condition").build()
        self.assertIn("Name must be specified when using case condition", str(cm.exception))

    def test_validation_then_without_case(self) -> None:
        """Test validation fails when then is used without case"""
        with self.assertRaises(ValidationError) as cm:
            count.then("value").name("test").build()
        self.assertIn("THEN clause specified without CASE condition", str(cm.exception))


class TestBuildValueSpecsDecorator(unittest.TestCase):
    def setUp(self) -> None:
        @build_value_specs
        def test_func(values: Any) -> Any:
            return values
        self.test_func = test_func

    def test_single_builder(self) -> None:
        """Test decorator with single AggregationBuilder"""
        builder = count.column("test").name("test_name")
        result = self.test_func(values=builder)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['name'], "test_name")

    def test_builder_list(self) -> None:
        """Test decorator with list of builders"""
        builders = [
            count.column("test1").name("name1"),
            sum.column("test2").name("name2")
        ]
        result = self.test_func(values=builders)
        self.assertIsInstance(result, list)
        self.assertTrue(all(isinstance(item, dict) for item in result))
        self.assertEqual([item['name'] for item in result], ["name1", "name2"])

    def test_mixed_list(self) -> None:
        """Test decorator with mixed list of builders and regular values"""
        values = [
            count.column("test").name("test_name"),
            {"existing": "dict"},
            "plain string"
        ]
        result = self.test_func(values=values)
        self.assertIsInstance(result[0], dict)
        self.assertEqual(result[1], {"existing": "dict"})
        self.assertEqual(result[2], "plain string")

    def test_non_builder_value(self) -> None:
        """Test decorator with non-builder value"""
        original = {"test": "value"}
        result = self.test_func(values=original)
        self.assertEqual(result, original)


if __name__ == '__main__':
    unittest.main()
