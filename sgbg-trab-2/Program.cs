using System;
using System.IO;
using sgbg_trab_2.Models;
using sgbg_trab_2.Operations;

class Program
{
    static void Main(string[] args)
    {
        try
        {
            Table vinho = new Table("vinho.csv");
            Table uva = new Table("uva.csv");
            Table pais = new Table("pais.csv");

            vinho.LoadData();
            uva.LoadData();
            pais.LoadData();

            var results = new List<JoinResult>();

            //results.Add(new JoinResult("Uva join Vinho", new Operator(uva, vinho, "uva_id", "uva_id")));
            results.Add(new JoinResult("Vinho join Uva", new Operator(vinho, uva, "uva_id", "uva_id")));
            //results.Add(new JoinResult("Uva join País", new Operator(uva, pais, "pais_origem_id", "pais_id")));
            //results.Add(new JoinResult("Vinho join País", new Operator(vinho, pais, "pais_producao_id", "pais_id")));

            foreach (var result in results)
                result.Op.Execute();

            Printer.PrintJoinSummary(results);

            string projectRoot = AppContext.BaseDirectory;
            string solutionRoot = Path.GetFullPath(Path.Combine(projectRoot, @"..\..\..\.."));
            string outDir = Path.Combine(solutionRoot, "out");

            Printer.SaveResults(results, outDir);

            Console.WriteLine("All operations completed successfully!");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            Console.WriteLine($"Stack Trace: {e.StackTrace}");
        }
    }

}