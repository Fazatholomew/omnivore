from omnivore.cambridge.cambridge import (
    cambridge,
    cambridge_general_process,
    quote_column_mapper,
    consulting_column_mapper,
    exclude_consulting_column,
    exclude_quote_column,
)
from .cambridge_data import (
    input_consulting,
    input_quote,
)


def test_general_cambridge_processing_function():
    consulting = cambridge_general_process(
        input_consulting, consulting_column_mapper, exclude_consulting_column
    )
    for before_column, after_column in consulting_column_mapper.items():
        assert (
            before_column not in consulting.columns
        ), f"\nColumn = '{before_column}' should not be in Cambridge Consulting"
        assert (
            after_column in consulting.columns
        ), f"\nColumn = '{after_column}' should be in Cambridge Consulting"
    quote = cambridge_general_process(
        input_quote, quote_column_mapper, exclude_quote_column
    )
    for before_column, after_column in quote_column_mapper.items():
        assert (
            before_column not in quote.columns
        ), f"\nColumn = '{before_column}' should not be in Cambridge Quote"
        assert (
            after_column in quote.columns
        ), f"\nColumn = '{after_column}' should be in Cambridge Quote"


def test_cambridge_processing_function():
    consulting = cambridge(input_consulting, input_quote)
    for before_column, after_column in list(consulting_column_mapper.items()) + list(
        quote_column_mapper.items()
    ):
        assert (
            before_column not in consulting.columns
        ), f"\nColumn = '{before_column}' should not be in Cambridge"
        assert (
            after_column in consulting.columns
        ), f"\nColumn = '{after_column}' should be in Cambridge"
