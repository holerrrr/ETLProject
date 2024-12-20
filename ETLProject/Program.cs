using System;
using System.Data;
using System.Globalization;
using System.IO;
using Microsoft.Data.SqlClient;

class Program
{
    static void Main(string[] args)
    {
        string csvFilePath = @"sample-cab-data.csv";
        string connectionString = "Server=localhost;Database=ETLProject;Trusted_Connection=True;Encrypt=False;";

        try
        {
            DataTable dataTable = ProcessCsv(csvFilePath);
            BulkInsertToSql(connectionString, dataTable);

            RemoveDuplicates(connectionString);

        }
        catch (Exception ex)
        {
            Console.WriteLine("Data successfully added.");

        }
    }

    static DataTable ProcessCsv(string filePath)
    {
        DataTable dataTable = new DataTable();

        dataTable.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
        dataTable.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
        dataTable.Columns.Add("passenger_count", typeof(int));
        dataTable.Columns.Add("trip_distance", typeof(decimal));
        dataTable.Columns.Add("store_and_fwd_flag", typeof(string));
        dataTable.Columns.Add("PULocationID", typeof(int));
        dataTable.Columns.Add("DOLocationID", typeof(int));
        dataTable.Columns.Add("fare_amount", typeof(decimal));
        dataTable.Columns.Add("tip_amount", typeof(decimal));

        using (var reader = new StreamReader(filePath))
        {
            string? line;
            bool isHeader = true;

            while ((line = reader.ReadLine()) != null)
            {
                if (isHeader)
                {
                    isHeader = false;
                    continue;
                }

                var parsedRow = ParseCsvLine(line);
                if (parsedRow != null)
                {
                    for (int i = 0; i < parsedRow.Length; i++)
                    {
                        if (parsedRow[i] is string strValue)
                        {
                            parsedRow[i] = strValue.Trim();
                        }
                    }
                    dataTable.Rows.Add(parsedRow);
                }
                else
                {
                    Console.WriteLine($"Error in line: {line}");
                }
            }
        }

        return dataTable;
    }


    static object[]? ParseCsvLine(string line)
    {
        var values = line.Split(',');

        if (values.Length < 14)
        {
            return null;
        }

        return new object[]
        {
            ConvertToUtc(ParseDate(values[1])),
            ConvertToUtc(ParseDate(values[2])), 
            ParseInt(values[3]), 
            ParseDecimal(values[4]), 
            ParseFlag(values[6]), 
            ParseInt(values[7]), 
            ParseInt(values[8]), 
            ParseDecimal(values[10]), 
            ParseDecimal(values[13]) 
        };
    }

    static DateTime? ParseDate(string value)
    {
        if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime result))
        {
            return result;
        }
        return null;
    }

    static DateTime ConvertToUtc(DateTime? estTime)
    {
        if (estTime == null) return DateTime.MinValue;
        TimeZoneInfo est = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        return TimeZoneInfo.ConvertTimeToUtc(estTime.Value, est);
    }

    static int ParseInt(string value)
    {
        return int.TryParse(value, out int result) ? result : 0;
    }

    static decimal ParseDecimal(string value)
    {
        return decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out decimal result) ? result : 0;
    }

    static string ParseFlag(string value)
    {
        return value.Trim() == "N" ? "No" : "Yes";
    }

    static void BulkInsertToSql(string connectionString, DataTable dataTable)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            connection.Open();

            string duplicatesFilePath = @"duplicates.csv";
            using (StreamWriter writer = new StreamWriter(duplicatesFilePath))
            {
                writer.WriteLine("tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,store_and_fwd_flag,PULocationID,DOLocationID,fare_amount,tip_amount");

                foreach (DataRow row in dataTable.Rows)
                {
                    DateTime pickupDateTime = (DateTime)row["tpep_pickup_datetime"];
                    DateTime dropoffDateTime = (DateTime)row["tpep_dropoff_datetime"];
                    int passengerCount = (int)row["passenger_count"];

                    bool existsInDb = CheckIfRecordExists(connection, pickupDateTime, dropoffDateTime, passengerCount);

                    if (existsInDb)
                    {
                        writer.WriteLine(string.Join(",", row.ItemArray));
                    }
                    else
                    {
                        InsertRecordToDb(connection, row);
                    }
                }
            }
        }
    }

    static bool CheckIfRecordExists(SqlConnection connection, DateTime pickupDateTime, DateTime dropoffDateTime, int passengerCount)
    {
        string query = @"
            SELECT COUNT(*) 
            FROM ProcessedTrips 
            WHERE tpep_pickup_datetime = @pickup_datetime
            AND tpep_dropoff_datetime = @dropoff_datetime
            AND passenger_count = @passenger_count
        ";

        using (SqlCommand cmd = new SqlCommand(query, connection))
        {
            cmd.Parameters.AddWithValue("@pickup_datetime", pickupDateTime);
            cmd.Parameters.AddWithValue("@dropoff_datetime", dropoffDateTime);
            cmd.Parameters.AddWithValue("@passenger_count", passengerCount);

            int count = (int)cmd.ExecuteScalar();
            return count > 0;
        }
    }

    static void InsertRecordToDb(SqlConnection connection, DataRow row)
    {
        string query = @"
            INSERT INTO ProcessedTrips (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, store_and_fwd_flag, PULocationID, DOLocationID, fare_amount, tip_amount)
            VALUES (@pickup_datetime, @dropoff_datetime, @passenger_count, @trip_distance, @store_and_fwd_flag, @PULocationID, @DOLocationID, @fare_amount, @tip_amount)
        ";

        using (SqlCommand cmd = new SqlCommand(query, connection))
        {
            cmd.Parameters.AddWithValue("@pickup_datetime", row["tpep_pickup_datetime"]);
            cmd.Parameters.AddWithValue("@dropoff_datetime", row["tpep_dropoff_datetime"]);
            cmd.Parameters.AddWithValue("@passenger_count", row["passenger_count"]);
            cmd.Parameters.AddWithValue("@trip_distance", row["trip_distance"]);
            cmd.Parameters.AddWithValue("@store_and_fwd_flag", row["store_and_fwd_flag"]);
            cmd.Parameters.AddWithValue("@PULocationID", row["PULocationID"]);
            cmd.Parameters.AddWithValue("@DOLocationID", row["DOLocationID"]);
            cmd.Parameters.AddWithValue("@fare_amount", row["fare_amount"]);
            cmd.Parameters.AddWithValue("@tip_amount", row["tip_amount"]);

            cmd.ExecuteNonQuery();
        }
    }

    static void RemoveDuplicates(string connectionString)
    {
        string query = @"
            DELETE FROM ProcessedTrips
            WHERE (tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count) IN (
                SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
                FROM ProcessedTrips
                GROUP BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count
                HAVING COUNT(*) > 1
            );
        ";

        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            connection.Open();

            using (SqlCommand cmd = new SqlCommand(query, connection))
            {
                cmd.ExecuteNonQuery();
            }
        }
    }
}
