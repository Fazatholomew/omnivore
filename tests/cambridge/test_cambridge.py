from omnivore.cambridge.cambridge import (
    cambridge,
    quote_column_mapper,
    consulting_column_mapper,
)
from .cambridge_data import (
    input_consulting,
    input_quote,
)
from pandas import isna


def test_cambridge_processing_function():
    consulting = cambridge(input_consulting, input_quote)
    for before_column, after_column in consulting_column_mapper.items():
        assert (
            before_column not in consulting.columns
        ), f"\nColumn = '{before_column}' should not be in Cambridge Consulting"
        assert (
            after_column in consulting.columns
        ), f"\nColumn = '{after_column}' should be in Cambridge Consulting"

    # for before_column, after_column in quote_column_mapper.items():
    #     assert (
    #         before_column not in quote.columns
    #     ), f"\nColumn = '{before_column}' should not be in Cambridge Quote"
    #     assert (
    #         after_column in quote.columns
    #     ), f"\nColumn = '{after_column}' should be in Cambridge Quote"
