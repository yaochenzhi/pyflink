from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table.descriptors import FileSystem, Schema, OldCsv, Kafka, Json, Rowtime
from pyflink.table import BatchTableEnvironment, StreamTableEnvironment, TableConfig, DataTypes,CsvTableSink
from pyflink.table.window import Tumble
import os
"""
class pyflink.table.descriptors.Rowtime[source]
    Rowtime descriptor for describing an event time attribute in the schema.

class pyflink.table.window.Tumble[source]
    Helper class for creating a tumbling window. Tumbling windows are consecutive, non-overlapping windows of a specified fixed length. For example, a tumbling window of 5 minutes size groups elements in 5 minutes intervals.
"""


def main():
    exec_env = StreamExecutionEnvironment.get_execution_environment()
    exec_env.set_parallelism(1)
    exec_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    t_config = TableConfig()
    t_env = StreamTableEnvironment.create(exec_env, t_config)

    mySink_file = "/tmp/mySink_file.txt"
    if os.path.exists(result_file):
        os.remove(result_file)

    t_env.connect(
                Kafka()
                    .version("0.11")
                    .topic("user")
                    .start_from_earliest()
                    .property("zookeeper.connect", "localhost:2181")
                    .property("bootstrap.servers", "localhost:9092")

            ).with_format(
                Json()
                    .fail_on_missing_field(True)
                    .json_schema(
                       "{"
                       "  type: 'object',"
                       "  properties: {"
                       "    a: {"
                       "      type: 'string'"
                       "    },"
                       "    b: {"
                       "      type: 'string'"
                       "    },"
                       "    c: {"
                       "      type: 'string'"
                       "    },"
                       "    time: {"
                       "      type: 'string',"
                       "      format: 'date-time'"
                       "    }"
                       "  }"
                       "}"
                )

            ).with_schema(
                Schema()
                    .field("rowtime", DataTypes.TIMESTAMP())
                    .rowtime(
                        Rowtime()
                        .timestamps_from_field("time")
                        .watermarks_periodic_bounded(60000)
                    )
                    .field("a", DataTypes.STRING())
                    .field("b", DataTypes.STRING())
                    .field("c", DataTypes.STRING())

            ).in_append_mode().register_table_source("mySource")

    t_env.register_table_sink(
        "mySink",
        CsvTableSink(
            ["a", "b"],
            [DataTypes.STRING(),
            DataTypes.STRING()],
            mySink_file
        )
    )

    t_env.scan("mySource") \
        .window(
            Tumble.over("1.hours").on("rowtime").alias("w")
        ) \
        .group_by("w, a") \
        .select("a, max(b)") \
        .insert_into("`mySink`")

    t_env.execute("myStreamJob")


if __name__ == '__main__':
    main()
