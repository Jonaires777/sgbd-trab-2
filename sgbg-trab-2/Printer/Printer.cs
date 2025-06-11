using System;
using System.Collections.Generic;
using System.IO;

public static class Printer
{
    public static void PrintJoinSummary(List<JoinResult> results)
    {
        foreach (var result in results)
        {
            Console.WriteLine($"=== {result.Name} ===");
            Console.WriteLine($"Pages: {result.Op.GetPagesCount()}, IOs: {result.Op.GetIOCount()}, Tuples: {result.Op.GetTuplesCount()}");
            Console.WriteLine();
        }

        Console.WriteLine("=== GENERAL RESUME ===");
        int totalPages = results.Sum(r => r.Op.GetPagesCount());
        int totalIOs = results.Sum(r => r.Op.GetIOCount());
        int totalTuples = results.Sum(r => r.Op.GetTuplesCount());
        Console.WriteLine($"Total - Pages: {totalPages}, IOs: {totalIOs}, Tuples: {totalTuples}");
        Console.WriteLine();
    }

    public static void SaveResults(List<JoinResult> results, string outDir)
    {
        Directory.CreateDirectory(outDir);
        foreach (var result in results)
        {
            string fileName = result.Name.ToLower().Replace(" ", "_") + ".csv";
            string path = Path.Combine(outDir, fileName);
            result.Op.SaveTuplesToFile(path);
        }
    }
}
