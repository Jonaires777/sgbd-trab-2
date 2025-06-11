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

        private const int BUFFER_SIZE = 4;
        private const int SORT_BUFFERS = 3;
        private const int OUTPUT_BUFFER = 1;
        private const int TUPLES_PER_PAGE = 10;

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

            var firstSortedRuns = SortTableWithBuffers(_firstTable, _firstColumn);
            var secondSortedRuns = SortTableWithBuffers(_secondTable, _secondColumn);

            ExecuteMergeJoinWithBuffers(firstSortedRuns, secondSortedRuns);

            CleanData();

            Console.WriteLine($"Join finished. {_tuplesCount} tuples generated");
            Console.WriteLine();
        }

        private List<string> SortTableWithBuffers(Table table, string columnName)
        {
            Console.WriteLine($"Sorting table {table.Name} by column {columnName}");

            var initalRuns = CreateInitialRuns(table, columnName);

            var finalRuns = MergeRunsWithBuffers(initalRuns, columnName);

            return finalRuns;
        }

        private List<String> CreateInitialRuns(Table table, string columnName)
        {
            var runs = new List<string>();
            var bufferPages = new List<Page>();
            int bufferTupleCount = 0;
            
            foreach(var page in table.Pages)
            {
                _IOCount++;
                bufferPages.Add(page);
                bufferTupleCount += page.OccupiedTuples;

                if (bufferPages.Count >= SORT_BUFFERS ||
                    bufferTupleCount >= SORT_BUFFERS * TUPLES_PER_PAGE)
                {
                    var run = CreateSortedRun(bufferPages, columnName);
                    runs.Add(run);
                    bufferPages.Clear();
                    bufferTupleCount = 0;
                }
            }

            if (bufferPages.Count > 0)
            {
                var run = CreateSortedRun(bufferPages, columnName);
                runs.Add(run);
            }

            return runs;
        }

        private string CreateSortedRun(List<Page> pages, string columnName)
        {
            var allTuples = new List<Models.Tuple>();

            foreach (var page in pages)
            {
                allTuples.AddRange(page.GetTuples());
            }

            allTuples.Sort((t1, t2) => CompareTuple(t1, t2, columnName));

            var outFile = Path.GetTempFileName();
            _tempFiles.Add(outFile);

            using (var writer = new StreamWriter(outFile))
            {
                foreach (var tuple in allTuples)
                {
                    writer.WriteLine(SerializeTuple(tuple));
                }
            }

            _IOCount++;
            return outFile;
        }

        private List<string> MergeRunsWithBuffers(List<string> runs, string columnName)
        {
            while(runs.Count > 1)
            {
                var newRuns = new List<string>();

                int buffersNeeded = Math.Min(SORT_BUFFERS, runs.Count) + OUTPUT_BUFFER;
                if (buffersNeeded > BUFFER_SIZE)
                {
                    Console.WriteLine($"Warning: Buffers needed ({buffersNeeded}) exceeds available ({BUFFER_SIZE})");
                }

                for (int i = 0; i < runs.Count; i += SORT_BUFFERS)
                {
                    var group = runs.Skip(i).Take(SORT_BUFFERS).ToList();
                    var mergedRun = MergeRunsGroup(group, columnName);
                    newRuns.Add(mergedRun);
                }

                foreach (var oldRun in runs)
                {
                    if (!newRuns.Contains(oldRun))
                    {
                        SafeDeleteFile(oldRun);
                    }
                }

                runs = newRuns;
            }

            return runs;
        }

        private string MergeRunsGroup(List<string> runs, string columname)
        {
            var outfile = Path.GetTempFileName();
            _tempFiles.Add(outfile);

            var readers = new List<StreamReader>();
            var inputBuffers = new List<Queue<Models.Tuple>>();
            var outputBuffer = new List<Models.Tuple>();

            try
            {
                foreach(var run in runs)
                {
                    _IOCount++;
                    var reader = new StreamReader(run);
                    readers.Add(reader);

                    var buffer = new Queue<Models.Tuple>();
                    LoadBufferFromRun(reader, buffer, TUPLES_PER_PAGE);
                    inputBuffers.Add(buffer);
                }

                using (var writer = new StreamWriter(outfile))
                {
                    while(inputBuffers.Any(b => b.Count > 0))
                    {
                        Models.Tuple minTuple = null;
                        int minBufferIndex = -1;

                        for (int i = 0; i < inputBuffers.Count; i++)
                        {
                            if (inputBuffers[i].Count > 0)
                            {
                                var currentTuple = inputBuffers[i].Peek();
                                if (minTuple == null 
                                    || CompareTuple(currentTuple, minTuple, columname) < 0)
                                {
                                    minTuple = currentTuple;
                                    minBufferIndex = i;
                                }
                            }
                        }

                        if (minTuple != null)
                        {
                            inputBuffers[minBufferIndex].Dequeue();

                            outputBuffer.Add(minTuple);

                            if (outputBuffer.Count >= OUTPUT_BUFFER * TUPLES_PER_PAGE)
                            {
                                WriteOutputBuffer(writer, outputBuffer);
                                outputBuffer.Clear();
                            }

                            if (inputBuffers[minBufferIndex].Count == 0)
                            {
                                LoadBufferFromRun(readers[minBufferIndex],
                                                inputBuffers[minBufferIndex],
                                                TUPLES_PER_PAGE);
                            }
                        }
                    }

                    if (outputBuffer.Count > 0)
                    {
                        WriteOutputBuffer(writer, outputBuffer);
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

            int pagesWritten = (int)Math.Ceiling((double)outputBuffer.Count / TUPLES_PER_PAGE);
            _IOCount += pagesWritten;
            return outfile;
        }

        private void LoadBufferFromRun(StreamReader reader, Queue<Models.Tuple> buffer, int maxTuples)
        {
            int count = 0;
            string line;

            int maxTuplesToLoad = Math.Min(maxTuples, TUPLES_PER_PAGE);

            while (count < maxTuplesToLoad && (line = reader.ReadLine()) != null)
            {
                buffer.Enqueue(DeserializeTuple(line));
                count++;
            }
        }

        private void WriteOutputBuffer(StreamWriter writer, List<Models.Tuple> outputBuffer)
        {
            foreach (var tuple in outputBuffer)
            {
                writer.WriteLine(SerializeTuple(tuple));
            }

            int pagesWritten = (int)Math.Ceiling((double)outputBuffer.Count / TUPLES_PER_PAGE);
            _IOCount += pagesWritten;
        }

        private void ExecuteMergeJoinWithBuffers(List<string> firstRuns, List<string> secondRuns)
        {
            var firstSorted = LoadSortedTuples(firstRuns[0]);
            var secondSorted = LoadSortedTuples(secondRuns[0]);

            int i = 0, j = 0;
            var outputBuffer = new List<Models.Tuple>();

            while (i < firstSorted.Count && j < secondSorted.Count)
            {
                var firstValue = firstSorted[i].GetValue(_firstColumn);
                var secondValue = secondSorted[j].GetValue(_secondColumn);

                int comparison = CompareValues(firstValue, secondValue);

                if (comparison == 0)
                {
                    int startJ = j;

                    while (i < firstSorted.Count &&
                           CompareValues(firstSorted[i].GetValue(_firstColumn), firstValue) == 0)
                    {
                        j = startJ;

                        while (j < secondSorted.Count &&
                               CompareValues(secondSorted[j].GetValue(_secondColumn), secondValue) == 0)
                        {
                            var mergedTuple = CreateMergedTuple(firstSorted[i], secondSorted[j]);

                            _resultTuples.Add(mergedTuple);
                            _tuplesCount++;

                            outputBuffer.Add(mergedTuple);
                            if (outputBuffer.Count >= OUTPUT_BUFFER * TUPLES_PER_PAGE)
                            {
                                int pages = (int)Math.Ceiling((double)outputBuffer.Count / TUPLES_PER_PAGE);
                                _pagesCount += pages;
                                _IOCount += pages;
                                outputBuffer.Clear();
                            }

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

            if (outputBuffer.Count > 0)
            {
                int pages = (int)Math.Ceiling((double)outputBuffer.Count / TUPLES_PER_PAGE);
                _pagesCount += pages;
                _IOCount += pages;
            }
        }

        private Models.Tuple CreateMergedTuple(Models.Tuple first, Models.Tuple second)
        {
            var mergedTuple = new Models.Tuple();

            foreach (var kv in first.Cols)
            {
                mergedTuple.Cols[kv.Key] = kv.Value;
            }

            foreach (var kv in second.Cols)
            {
                if (!mergedTuple.Cols.ContainsKey(kv.Key))
                {
                    mergedTuple.Cols[kv.Key] = kv.Value;
                }
            }

            return mergedTuple;
        }

        private List<Models.Tuple> LoadSortedTuples(string filePath)
        {
            var sortedTuples = new List<Models.Tuple>();
            int pagesRead = (int)Math.Ceiling((double)sortedTuples.Count / TUPLES_PER_PAGE);
            _IOCount += pagesRead;


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