from chispa.dataframe_comparer import *
from collections import namedtuple
from src.jobs.actors_scd_job import actors_scd_transformation

Actor = namedtuple("Actor", "actor actorid quality_class is_active current_year")
ActorSCD = namedtuple("ActorSCD", "actor actorid quality_class is_active start_year end_year")

def test_scd_generation(spark):  
    source_data = [
        Actor("Herman J. Mankiewicz", "nm0542534", "average", True,  1928),
        Actor("Herman J. Mankiewicz", "nm0542534", "average", False,  1929),
        Actor("Herman J. Mankiewicz", "nm0542534", "average", False,  1930),
        Actor("Herman J. Mankiewicz", "nm0542534",  "average", True,  1931),
        Actor("Herman J. Mankiewicz", "nm0542534", "average", False,  1932),
        Actor("Herman J. Mankiewicz", "nm0542534", "average", False,  1933),
        Actor("Phil Harris", "nm0365201", "good", True,  1929)
    ]
    source_df = spark.createDataFrame(source_data)
    print(source_df)
    actual_df = actors_scd_transformation(spark, source_df)
    expected_data = [
        ActorSCD("Herman J. Mankiewicz", "nm0542534", "average", True, 1928, 1928),
        ActorSCD("Herman J. Mankiewicz", "nm0542534", "average", False, 1929, 1930),
        ActorSCD("Herman J. Mankiewicz", "nm0542534", "average", True, 1931, 1931),
        ActorSCD("Herman J. Mankiewicz", "nm0542534", "average", False, 1932, 1933),
        ActorSCD("Phil Harris", "nm0365201", "good", True,  1929, 1929)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)

