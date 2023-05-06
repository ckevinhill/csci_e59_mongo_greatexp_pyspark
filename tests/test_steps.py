"""TestCase for Steps"""
from gdh_hello_world_pipeline.steps import ConcatenateColumnsStep
from gdh_hello_world_pipeline.steps import UniqueCountStep


def test_add_column_step(correct_test_data_frame):
    """Test HelloWorldStep."""

    col_name = "new_col_name"
    step = ConcatenateColumnsStep("id", "letter", col_name)
    output_df = step.run(correct_test_data_frame)

    assert col_name in output_df.columns
    assert output_df.count() == correct_test_data_frame.count()


def test_count_step(correct_test_data_frame):
    """Test UniqueCountStep."""

    grp_col = "id"
    cnt_col = "cnt"
    step = UniqueCountStep(col=grp_col, cnt_col_name=cnt_col)
    output_df = step.run(correct_test_data_frame)

    assert cnt_col in output_df.columns
