using System;
using System.IO;
using Mono.Options;

namespace output2db
{
    public static partial class Program
    {
        private static readonly char[] s_spaceDelim = new[] { ' ' };
        private const string DEFAULT_DB_SERVER = "localhost";
        private const string DEFAULT_DB_USER = "wbpoc";
        private const string DEFAULT_DB_PWD = "sql@tfs2008";

        static int Main(string[] args)
        {
            try
            {
                return Run(args);
            }
            catch (Exception exc)
            {
                Console.Error.WriteLine(exc);
                return 2;
            }
        }

        static int Run(string[] args)
        {
            bool trusted = false;
            bool reset = false;
            string connectionString = null;
            string database = null;
            string server = null;
            string user = null;
            string pwd = null;
            string filePath = null;
            string table = null;
            bool help = false;
            var defDBName = $"_{DateTime.Now:yyyyMMdd}";
            int batchSize = 10000;
            bool verbose = false;
            var optionSet = new OptionSet()
                .Add("Dumps dumpheap output to a database")
                .Add("h|?|help", "Prints this help screen", _ => help = true)
                .Add("v|verbose", "Verbose output", _ => verbose = true)
                .Add("f=|file=", "[REQUIRED] The log file produced by WinDBG capturing the output of the !dumpheap command", v => filePath = v)
                .Add("c=|conn=", "The DB connection string", v => connectionString = v)
                .Add("db=|database=", "The DB name. Defaults to '_' + the current date, which is " + defDBName + " today. Mutually exclusive with --conn", v => database = v)
                .Add("s=|server=", "The DB server. The default value is " + DEFAULT_DB_SERVER + ". Mutually exclusive with --conn", v => server = v)
                .Add("u=|user=", "The DB user. The default is " + DEFAULT_DB_USER + ". Mutually exclusive with --conn", v => user = v)
                .Add("p=|pwd=", "The DB password. The default is " + DEFAULT_DB_PWD + ". Mutually exclusive with --conn", v => pwd = v)
                .Add("t=|table=", "The table name. Defaults to the file name without the extension.", v => table = v)
                .Add("reset", "Drop the table, if already exists. The normal behavior is to skip the table creation, if already exists.", _ => reset = true)
                .Add("trusted", "Trusted connection - use the current user credentials. Mutually exclusive with --conn, --user and --pwd", _ => trusted = true)
                .Add("bs=|batchSize=", "The batch size. Defaults to " + batchSize, (int v) => batchSize = v)
                ;

            var extra = optionSet.Parse(args);
            if (args.Length == 0 || help)
            {
                optionSet.WriteOptionDescriptions(Console.Out);
                return 0;
            }

            if (extra.Count > 0)
            {
                Console.WriteLine("Unknown command line arguments: " + string.Join(" ", extra));
                optionSet.WriteOptionDescriptions(Console.Out);
                return 1;
            }

            if (filePath == null)
            {
                Console.Error.WriteLine("Parameter --file is required");
                optionSet.WriteOptionDescriptions(Console.Error);
                return 1;
            }

            if (!File.Exists(filePath))
            {
                Console.Error.WriteLine($"The file {filePath} does not exist");
                optionSet.WriteOptionDescriptions(Console.Error);
                return 1;
            }

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                if (string.IsNullOrWhiteSpace(database))
                {
                    database = defDBName;
                }
                if (trusted && (user != null || pwd != null))
                {
                    Console.Error.WriteLine("The parameter --trusted is mutually exclusive with --user and --pwd");
                    optionSet.WriteOptionDescriptions(Console.Error);
                    return 1;
                }

                connectionString = trusted ?
                    $"server={server ?? DEFAULT_DB_SERVER};database={database};Trusted_Connection=True" :
                    $"server={server ?? DEFAULT_DB_SERVER};database={database};uid={user ?? DEFAULT_DB_USER};pwd={pwd ?? DEFAULT_DB_PWD}";
            }
            else if (!string.IsNullOrWhiteSpace(server) || !string.IsNullOrWhiteSpace(database) || !string.IsNullOrWhiteSpace(user) || !string.IsNullOrWhiteSpace(pwd) || trusted)
            {
                Console.Error.WriteLine("Parameters --server, --database, --user, --pwd and --trusted are mutually exclusive with --conn");
                optionSet.WriteOptionDescriptions(Console.Error);
                return 1;
            }
            if (table == null)
            {
                table = Path.GetFileNameWithoutExtension(filePath);
            }

            if (verbose)
            {
                Console.WriteLine("Database: {0}", database);
                Console.WriteLine("   Table: {0}", table);
                Console.WriteLine("  Server: {0}", server ?? DEFAULT_DB_SERVER);
                Console.WriteLine(" Trusted: {0}", trusted);
            }

            JournalEntry j;
            using (var conn = SetupDB(connectionString, filePath, table, reset, out j, verbose))
            {
                if (j.PercentDone < 100)
                {
                    var fileLength = new FileInfo(j.FilePath).Length;
                    Action<long> fnWriteProgress = byteOffset => { };
                    if (verbose)
                    {
                        if (j.LineOffset > 1)
                        {
                            Console.WriteLine("Resuming from line {0}", j.LineOffset);
                        }
                        else
                        {
                            Console.WriteLine("Starting from the beginning");
                        }
                        fnWriteProgress = byteOffset => WriteProgress(byteOffset, fileLength);
                    }
                    PersistDumpHeap(conn, j, table, batchSize, fileLength, fnWriteProgress);
                }
                else if (verbose)
                {
                    Console.WriteLine("Already done.", filePath);
                }
            }
            return 0;
        }

        private static void WriteProgress(long byteOffset, long fileLength)
        {
            if (byteOffset < -1)
            {
                Console.WriteLine();
                return;
            }

            if (byteOffset < 0)
            {
                Console.Write("   100.00%");
            }
            else
            {
                Console.Write("   {0:0.00}%\r", byteOffset * 100.0 / fileLength);
            }
            Console.Out.Flush();
        }
    }
}
