# Introduction

The `SqlStreamStore.SchemaCreationScriptTool` can be used to generate the database schema creation script for the various SQL based implementations.
Currently we support the following command line argument values for the `sqldialect`:

- `mssqlv2` compatible with the now obsolete `SqlStreamStore.MsSql.MsSqlStreamStore` for Microsoft SQL Server (2012+)
- `mssqlv3` compatible with `SqlStreamStore.MsSql.MsSqlStreamStoreV3` for Microsoft SQL Server (2012+)
- `mysql` compatible with `SqlStreamStore.MySql.MySqlStreamStore` for Oracle MySql
- `postgres` compatible with `SqlStreamStore.Postgres.PostgresStreamStore` for Postgres

Next to that we also support specifying the database `schema` to generate the script for, using the `-s|--schema <SCHEMA>` command line option.
The creation of the database schema itself as DDL can be accomplished by specifying the `-cs|--create-schema` command line option.
Bare in mind these only applies to `mssqlv2`, `mssqlv3` and `postgres`.
The file path to output the script into can be specified using the `-o|--output <PATH>` command line option.

To install the tool, one can run the following command:

`dotnet tool install --global SqlStreamStore.SchemaCreationScriptTool`

**Note**: Beta version will require you to specify a `--version` as well.

Pinning the tool to the same version as the version of SqlStreamStore your code is using, will ensure the tool generates a schema creation script that is compatible with your code. 

From then on you should be able to run the tool itself with a command resembling:

`sqlstreamstore-schema-creation-script mssqlv3 -s MyEvents -cs -o MyEventsDatabaseScript.sql`   