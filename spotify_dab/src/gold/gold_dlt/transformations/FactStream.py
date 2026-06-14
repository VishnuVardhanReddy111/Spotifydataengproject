import dlt
from pyspark import pipelines as dp
@dlt.table
def factstream_stg():
    df = spark.readStream.table("spotify_cata.default.factstream")
    return df

dlt.create_streaming_table("factstream")

dp.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)