from unittest import TestCase

from sqlalchemy.types import NullType, VARCHAR

from sqlalchemy_redshift.dialect import RedshiftDialect


class TestColumnReflection(TestCase):
    def test_varchar_as_nulltype(self):
        """
        Varchar columns with no length should be considered NullType columns
        """
        dialect = RedshiftDialect()
        column_info = dialect._get_column_info(
            'Null Column',
            'character varying', None, False, {}, {}, 'default', 'test column'
        )
        assert isinstance(column_info['type'], NullType)
        column_info_1 = dialect._get_column_info(
            'character column',
            'character varying(30)', None, False, {}, {}, 'default',
            comment='test column'
        )
        assert isinstance(column_info_1['type'], VARCHAR)

    def test_encode_in_dialect_options(self):
        """
        Column encoding should be a dialect_kwarg under dialect_options
        """
        dialect = RedshiftDialect()
        column_info = dialect._get_column_info(
            'Column with encoding',
            'integer', None, False, {}, {}, 'default', encode='LZO',
            comment='test encoding column'
        )
        assert column_info['dialect_options']['redshift_encode'] == 'LZO'
