from pyflink.dataset import ExecutionEnvironment

from pyflink.datastream import StreamExecutionEnvironment

from pyflink.table import BatchTableEnvironment, StreamTableEnvironment, TableConfig, DataTypes

from pyflink.table.descriptors import FileSystem, Schema, OldCsv

"""
    Entry doc: 
        https://ci.apache.org/projects/flink/flink-docs-release-1.9/
    Entry doc for python:
        https://ci.apache.org/projects/flink/flink-docs-release-1.9/tutorials/python_table_api.html#python-api-tutorial
    API doc for pyflink: 
        https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/python/

    pyflink.table package:
        https://ci.apache.org/projects/flink/flink-docs-release-1.9/api/python/pyflink.table.html#pyflink.table.Table

    ---
    with_format(format_descriptor)[source]¶
        Specifies the format that defines how to read data from a connector.

        Returns
        This object.

    with_schema(schema)[source]¶
        Specifies the resulting table schema.

        Returns
        This object.

    ---
    table env container:
        contains:
            execution env
            table config
        connects:
            source
            sink
        graph:
            source to sink working graph
        
        execute to start work !
"""

def main():
    exec_env = ExecutionEnvironment.get_execution_environment()
    exec_env.set_parallelism(1)
    t_config = TableConfig()
    t_env = BatchTableEnvironment.create(exec_env, t_config)

    t_env.connect(FileSystem().path('/tmp/input'))        \
        .with_format(OldCsv()
                     .line_delimiter(' ')
                     .field('word', DataTypes.STRING()))  \
        .with_schema(Schema()
                     .field('word', DataTypes.STRING()))  \
        .register_table_source('mySource')

    t_env.connect(FileSystem().path('/tmp/output'))       \
        .with_format(OldCsv()
                     .field_delimiter('\t')
                     .field('word', DataTypes.STRING())
                     .field('count', DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field('word', DataTypes.STRING())
                     .field('count', DataTypes.BIGINT())) \
        .register_table_sink('mySink')

    t_env.scan('mySource')        \
        .group_by('word')         \
        .select('word, count(1)') \
        .insert_into('mySink')

    t_env.execute("tutorial_job")


if __name__ == '__main__':
    main()