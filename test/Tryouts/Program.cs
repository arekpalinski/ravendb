using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Voron.Util;
using Raven.Server.Utils;
using SlowTests.Corax;
using SlowTests.Issues;
using SlowTests.Server;
using SlowTests.Sharding.Cluster;
using Tests.Infrastructure;
using Xunit;

using System;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;

namespace Tryouts
{
    // --- POCOs (Plain Old CLR Objects) representing Northwind documents ---
    
    public class Address
    {
        public string City { get; set; }
        public string Country { get; set; }
    }

    public class Employee
    {
        public string Id { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public Address Address { get; set; }
    }

    public class Product
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public decimal PricePerUnit { get; set; }
        public int UnitsInStock { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {

            foreach (var dbName in new[] { "test", "aaa" })
            {

                // 1. Initialize the Document Store
                // Make sure your local RavenDB server is running on localhost:8080
                using var store = new DocumentStore
                {
                    Urls = new[] { "http://localhost:8080" },
                    Database = dbName
                }.Initialize();

                Console.WriteLine($"Connected to RavenDB '{dbName}' database.");

                // 2. Open a session to interact with the database
                using (var session = store.OpenSession())
                {
                    Console.WriteLine("\n--- Query 1: Simple Filtering (Nested Property) ---");
                    // Find all employees located in London
                    var londonEmployees = session.Query<Employee>()
                        .Where(e => e.Address.City == "London")
                        .ToList();

                    foreach (var emp in londonEmployees)
                    {
                        Console.WriteLine($"Employee: {emp.FirstName} {emp.LastName}, City: {emp.Address.City}");
                    }

                    Console.WriteLine("\n--- Query 2: Filtering, Ordering, and Paging ---");
                    // Find the top 5 most expensive products that are currently in stock
                    var expensiveProducts = session.Query<Product>()
                        .Where(p => p.UnitsInStock > 0)
                        .OrderByDescending(p => p.PricePerUnit)
                        .Take(5)
                        .ToList();

                    foreach (var prod in expensiveProducts)
                    {
                        Console.WriteLine($"Product: {prod.Name}, Price: ${prod.PricePerUnit}, Stock: {prod.UnitsInStock}");
                    }

                    Console.WriteLine("\n--- Query 3: String Searching ---");
                    // Find products where the name starts with "Ch"
                    var specificProducts = session.Advanced.RawQuery<Product>(@"from index 'Product/Search' where search(Name, 'ch')")
                        .ToList();

                    foreach (var prod in specificProducts)
                    {
                        Console.WriteLine($"Product Name: {prod.Name}");
                    }
                }

                Console.WriteLine("\nQueries executed successfully. Press any key to exit.");
                Console.ReadKey();
            }
        }
    }
}
