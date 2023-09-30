open System
open BenchmarkDotNet
open BenchmarkDotNet.Attributes
open Npgsql
open Testcontainers.PostgreSql

type CopyVsInsertUnnestComparison() =
    let mutable postgresContainer: PostgreSqlContainer = Unchecked.defaultof<_>
    let mutable dataSource: NpgsqlDataSource = Unchecked.defaultof<_>

    let data: ResizeArray<int * string * DateTime> = ResizeArray<_>()

    [<Params(1, 10, 100, 1000, 10000, 100000)>]
    member val Insertions = 0 with get, set

    [<Params("14.9", "15.4", "16.0")>]
    member val PostgresVersion = Unchecked.defaultof<_> with get, set

    [<GlobalSetup>]
    member this.GlobalSetup() =
        task {
            postgresContainer <- (new PostgreSqlBuilder()).WithImage($"postgres:{this.PostgresVersion}").Build()
            do! postgresContainer.StartAsync()

            dataSource <- (new NpgsqlDataSourceBuilder(postgresContainer.GetConnectionString())).Build()

            use command =
                dataSource.CreateCommand
                    """
                CREATE TABLE insertion_table (id INTEGER NOT NULL, name TEXT NOT NULL, date TIMESTAMP NOT NULL)"""

            let! _ = command.ExecuteNonQueryAsync()

            data.Clear()

            seq {
                for i in 1 .. this.Insertions do
                    Random.Shared.Next(),
                    IO.Path.GetRandomFileName(),
                    (DateTime.UnixEpoch.AddDays(Random.Shared.NextDouble() * 10000.0))
                        .ToUniversalTime()
            }
            |> data.AddRange
        }

    [<GlobalCleanup>]
    member this.GlobalCleanup() =
        task {
            do! dataSource.DisposeAsync()
            do! postgresContainer.StopAsync()
        }

    [<Benchmark>]
    member this.InsertUnnest() =
        task {
            use command =
                dataSource.CreateCommand
                    """
                INSERT INTO insertion_table (id, name, date)
                SELECT id, name, date
                FROM unnest(@ids, @names, @dates) AS t(id, name, date)
                """

            command.Parameters.Add(NpgsqlParameter<int[]>("ids", data |> Seq.map (fun (i, _, _) -> i) |> Seq.toArray))
            |> ignore

            command.Parameters.Add(
                NpgsqlParameter<string[]>("names", data |> Seq.map (fun (_, n, _) -> n) |> Seq.toArray)
            )
            |> ignore

            command.Parameters.Add(
                NpgsqlParameter<DateTime[]>("dates", data |> Seq.map (fun (_, _, d) -> d) |> Seq.toArray)
            )
            |> ignore

            let! _ = command.ExecuteNonQueryAsync()
            return ()
        }


    [<Benchmark>]
    member this.Copy() =
        task {
            use connection = dataSource.CreateConnection()
            do! connection.OpenAsync()

            let! importer =
                connection.BeginBinaryImportAsync
                    """
                COPY insertion_table (id, name, date)
                FROM STDIN (FORMAT BINARY)
                """

            for i, n, d in data do
                do! importer.StartRowAsync()
                do! importer.WriteAsync i
                do! importer.WriteAsync n
                do! importer.WriteAsync d

            let! _ = importer.CompleteAsync()
            return ()
        }

[<EntryPoint>]
let main argv =
    let summary =
        BenchmarkDotNet.Running.BenchmarkRunner.Run<CopyVsInsertUnnestComparison>()

    0
