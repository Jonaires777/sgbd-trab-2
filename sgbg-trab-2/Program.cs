using System;
using System.Reflection.Emit;
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

            Operator op = new Operator(vinho, uva, "vinho_id", "uva_id");

            op.Execute();

            Console.WriteLine($"#Pags: {op.GetPagesCount()}"); 
            Console.WriteLine($"#IOs: {op.GetIOCount()}"); 
            Console.WriteLine($"#Tups: {op.GetTuplesCount()}");

            string projectRoot = AppContext.BaseDirectory;
            string solutionRoot = Path.GetFullPath(Path.Combine(projectRoot, @"..\..\..\.."));
            string outDir = Path.Combine(solutionRoot, "out");

            Directory.CreateDirectory(outDir);

            string filePath = Path.Combine(outDir, "selecao_vinho_pos_operacoes.csv");
            op.SaveTuplesToFile(filePath);
        }
        catch (Exception e)
        {
            Console.WriteLine($"Error: {e.Message}");
            Console.WriteLine($"Stack Trace: {e.StackTrace}");
        }
    }
}
