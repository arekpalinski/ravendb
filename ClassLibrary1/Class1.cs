using System.Reflection;
using Microsoft.Coyote;
using Microsoft.Coyote.SystematicTesting;
using Raven.Client.Documents;
using Raven.Client2;
using Sparrow;
using Xunit;

namespace ClassLibrary1
{
    public class Class1
    {
        [Microsoft.Coyote.SystematicTesting.Test]
        public static async Task Execute2()
        {
            Size size = new Size(11, SizeUnit.Bytes);

            //Assembly.Load(typeof(DocumentStore).AssemblyQualifiedName);

            DocumentStore2 store2;
            DocumentStore store;

            int value = 0;
            Task task = Task.Run(() =>
            {
                value = 1;
            });

            Assert.Equal(0, value);
            await task;
        }

        [Fact]
        public void CoyoteTestTask()
        {
            var configuration = Configuration.Create().WithTestingIterations(100);
            var engine = TestingEngine.Create(configuration, Execute2);
            engine.Run();

            var report = engine.TestReport;
            Console.WriteLine("Coyote found {0} bug.", report.NumOfFoundBugs);
            Assert.True(report.NumOfFoundBugs == 0, $"Coyote found {report.NumOfFoundBugs} bug(s). . {report.BugReports.First()}");


            if (string.IsNullOrEmpty(engine.ReproducibleTrace) == false)
            {
                var config = Configuration.Create().WithReproducibleTrace(engine.ReproducibleTrace);
                TestingEngine engine2 = TestingEngine.Create(config, Execute2);
                engine2.Run();
            }
            Assert.True(report.NumOfFoundBugs == 0, $"Coyote found {report.NumOfFoundBugs} bug(s). . {report.BugReports.First()}");
        }
    }
}
