using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using sgbg_trab_2.Models;

namespace sgbg_trab_2.Operations
{
    public class Operator
    {
        private Table _firstTable;
        private Table _secondTable;
        private string _firstColumn;
        private string _secondColumn;
        private int _IOCount;
        private int _pagesCount;
        private int _tuplesCount;
        private List<Models.Tuple> _resultTuples;
        private List<string> _tempFiles;

        public Operator(Table firstTable, Table secondTable, string firstColumn, string secondColumn)
        {
            _firstTable = firstTable;
            _secondTable = secondTable;
            _firstColumn = firstColumn;
            _secondColumn = secondColumn;
            _IOCount = 0;
            _pagesCount = 0;
            _tuplesCount = 0;
            _resultTuples = new List<Models.Tuple>();
            _tempFiles = new List<string>();
        }

        public void Execute()
        {
            Console.WriteLine($"Executing Sort-Merge Join between {_firstTable.Name} e {_secondTable.Name}");
            Console.WriteLine($"Condition: {_firstTable.Name}.{_firstColumn} = {_secondTable.Name}.{_secondColumn}");

            var firstSortedTable = SortTable(_firstTable, _firstColumn);
            var secondSortedTable = SortTable(_secondTable, _secondColumn);

            ExecuteMergeJoin(firstSortedTable, secondSortedTable);

            CleanData();

            Console.WriteLine($"Join finished. {_tuplesCount} tuples generated");
        }

        private List<Models.Tuple> SortTable(Table table, string columnName)
        {
            Console.WriteLine($"Sorting table {table.Name} by column {columnName}");

            var runs = new List<string>();

            foreach (var page in table.Pages)
            {
                _IOCount++;

                var tuples = page.GetTuples();

                tuples.Sort((t1, t2) =>
                {
                    var val1 = t1.GetValue(columnName);
                    var val2 = t2.GetValue(columnName);

                    if (val1 == null && val2 == null) return 0;
                    if (val1 == null) return -1;
                    if (val2 == null) return 1;

                    return Comparer<object>.Default.Compare(val1, val2);
                });

                var nameRun = Path.GetTempFileName();
                _tempFiles.Add(nameRun);

                using (var writer = new StreamWriter(nameRun))
                {
                    foreach (var tupla in tuples)
                    {
                        writer.WriteLine(SerializeTuple(tupla));
                    }
                }

                _IOCount++;
                runs.Add(nameRun);
            }

            while (runs.Count > 1)
            {
                var newRuns = new List<string>();
                for (int i = 0; i < runs.Count; i += 3)
                {
                    var group = runs.Skip(i).Take(3).ToList();
                    var runMerged = MergeRuns(group, columnName);
                    newRuns.Add(runMerged);
                }

                // Clean up old runs that are no longer needed
                foreach (var oldRun in runs)
                {
                    if (!newRuns.Contains(oldRun))
                    {
                        SafeDeleteFile(oldRun);
                    }
                }

                runs = newRuns;
            }

            return LoadSortedTuples(runs[0]);
        }

        private string MergeRuns(List<string> runs, string columnName)
        {
            var outFile = Path.GetTempFileName();
            _tempFiles.Add(outFile);

            var readers = new List<StreamReader>();
            var currentTuples = new List<Models.Tuple>();

            try
            {
                foreach (var run in runs)
                {
                    _IOCount++;
                    var reader = new StreamReader(run);
                    readers.Add(reader);

                    var line = reader.ReadLine();
                    if (line != null)
                    {
                        currentTuples.Add(DeserializeTuple(line));
                    }
                    else
                    {
                        currentTuples.Add(null);
                    }
                }

                using (var writer = new StreamWriter(outFile))
                {
                    while (currentTuples.Any(t => t != null))
                    {
                        Models.Tuple minTuple = null;
                        int minIndex = -1;

                        for (int i = 0; i < currentTuples.Count; i++)
                        {
                            if (currentTuples[i] != null)
                            {
                                if (minTuple == null || CompareTuple(currentTuples[i], minTuple, columnName) < 0)
                                {
                                    minTuple = currentTuples[i];
                                    minIndex = i;
                                }
                            }
                        }

                        if (minTuple != null)
                        {
                            writer.WriteLine(SerializeTuple(minTuple));

                            var nextLine = readers[minIndex].ReadLine();
                            if (nextLine != null)
                            {
                                currentTuples[minIndex] = DeserializeTuple(nextLine);
                            }
                            else
                            {
                                currentTuples[minIndex] = null;
                            }
                        }
                    }
                }
            }
            finally
            {
                foreach (var reader in readers)
                {
                    reader?.Dispose();
                }
            }

            _IOCount++;
            return outFile;
        }

        private List<Models.Tuple> LoadSortedTuples(string filePath)
        {
            var sortedTuples = new List<Models.Tuple>();
            _IOCount++;

            using (var reader = new StreamReader(filePath))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    sortedTuples.Add(DeserializeTuple(line));
                }
            }

            return sortedTuples;
        }

        private void ExecuteMergeJoin(List<Models.Tuple> firstSorted, List<Models.Tuple> secondSorted)
        {
            int i = 0, j = 0;
            var actualPage = new Page();

            while (i < firstSorted.Count && j < secondSorted.Count)
            {
                var firstValue = firstSorted[i].GetValue(_firstColumn);
                var secondValue = secondSorted[j].GetValue(_secondColumn);

                int comparison = CompareValues(firstValue, secondValue);

                if (comparison == 0)
                {
                    int startJ = j;

                    while (i < firstSorted.Count && CompareValues(firstSorted[i].GetValue(_firstColumn), firstValue) == 0)
                    {
                        j = startJ;

                        while (j < secondSorted.Count && CompareValues(secondSorted[j].GetValue(_secondColumn), secondValue) == 0)
                        {
                            var mergedTuple = new Models.Tuple();

                            foreach (var kv in firstSorted[i].Cols)
                            {
                                mergedTuple.Cols[kv.Key] = kv.Value;
                            }

                            foreach (var kv in secondSorted[j].Cols)
                            {
                                if (!mergedTuple.Cols.ContainsKey(kv.Key))
                                {
                                    mergedTuple.Cols[kv.Key] = kv.Value;
                                }
                            }

                            _resultTuples.Add(mergedTuple);

                            if (!actualPage.AddTuple(mergedTuple))
                            {
                                _pagesCount++;
                                _IOCount++;
                                actualPage = new Page();
                                actualPage.AddTuple(mergedTuple);
                            }

                            _tuplesCount++;
                            j++;
                        }
                        i++;
                    }
                }
                else if (comparison < 0)
                {
                    i++;
                }
                else
                {
                    j++;
                }
            }
        }

        private int CompareTuple(Models.Tuple t1, Models.Tuple t2, string columnName)
        {
            var val1 = t1.GetValue(columnName);
            var val2 = t2.GetValue(columnName);
            return CompareValues(val1, val2);
        }

        private int CompareValues(object val1, object val2)
        {
            if (val1 == null && val2 == null) return 0;
            if (val1 == null) return -1;
            if (val2 == null) return 1;

            return Comparer<object>.Default.Compare(val1, val2);
        }

        private string SerializeTuple(Models.Tuple tuple)
        {
            var items = tuple.Cols.Select(kvp => $"{kvp.Key}={kvp.Value}");
            return string.Join(";", items);
        }

        private Models.Tuple DeserializeTuple(string line)
        {
            var tuple = new Models.Tuple();
            var items = line.Split(';');

            foreach (var item in items)
            {
                if (item.Contains('='))
                {
                    var parts = item.Split('=', 2);
                    var key = parts[0].Trim();
                    var value = parts[1].Trim();

                    if (int.TryParse(value, out int intValue))
                    {
                        tuple.Cols[key] = intValue;
                    }
                    else if (double.TryParse(value, out double doubleValue))
                    {
                        tuple.Cols[key] = doubleValue;
                    }
                    else
                    {
                        tuple.Cols[key] = value;
                    }
                }
            }

            return tuple;
        }

        private void SafeDeleteFile(string filePath)
        {
            if (File.Exists(filePath))
            {
                try
                {
                    File.Delete(filePath);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Could not delete temporary file {filePath}: {ex.Message}");
                }
            }
        }

        private void CleanData()
        {
            foreach (var file in _tempFiles)
            {
                SafeDeleteFile(file);
            }
            _tempFiles.Clear();
        }

        public int GetPagesCount()
        {
            return _pagesCount;
        }

        public int GetIOCount()
        {
            return _IOCount;
        }

        public int GetTuplesCount()
        {
            return _tuplesCount;
        }

        public void SaveTuplesToFile(string filePath)
        {
            if (_resultTuples.Count == 0)
            {
                Console.WriteLine("No result tuples to save.");
                return;
            }

            var columns = new HashSet<string>();
            foreach (var tuple in _resultTuples)
            {
                foreach (var col in tuple.Cols.Keys)
                {
                    columns.Add(col);
                }
            }

            var orderedColumns = columns.OrderBy(c => c).ToList();

            using (var writer = new StreamWriter(filePath))
            {
                writer.WriteLine(string.Join(",", orderedColumns));

                foreach (var tuple in _resultTuples)
                {
                    writer.WriteLine(tuple.ToCsvString(orderedColumns));
                }
            }

            Console.WriteLine($"Result tuples saved to {filePath}");
        }
    }
}