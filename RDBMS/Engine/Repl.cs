using System;
using System.Threading.Tasks;

namespace RDBMS.Engine
{
    public class Repl
    {
        private readonly Database _db;

        public Repl(Database db)
        {
            _db = db;
        }

        public void Start()
        {
            Task.Run(() => 
            {
                // Give the web server a second to start logs
                System.Threading.Thread.Sleep(1000); 
                Console.WriteLine("\n=================================");
                Console.WriteLine("   SQLly REPL READY");
                Console.WriteLine("   Try: INSERT INTO users VALUES (1, \"Admin\", 99)");
                Console.WriteLine("=================================\n");

                while (true)
                {
                    Console.Write("SQLly> ");
                    var input = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(input)) continue;
                    if (input == "exit") break;

                    var result = _db.ExecuteSql(input);
                    Console.WriteLine(result);
                }
            });
        }
    }
}