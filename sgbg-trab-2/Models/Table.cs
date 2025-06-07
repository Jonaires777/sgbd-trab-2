using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sgbg_trab_2.Models
{
    public class Table
    {
        public string Name { get; private set; }
        public List<Page> Pages { get; private set; }
        public int PageQuantity { get; private set; }
        public int ColumnsQuantity { get; private set; }
        public List<string> Columns { get; private set; }
        public string _filePath { get; private set; }

        public Table(string filePath)
        {
            Name = Path.GetFileNameWithoutExtension(filePath);
            Columns = new List<string>();
            Pages = new List<Page>();
            PageQuantity = 0;
            ColumnsQuantity = 0;
            _filePath = filePath;
        }

        public void LoadData() 
        {
            if (!File.Exists(_filePath))
            {
                Console.WriteLine($"File {_filePath} not found.");
                return;
            }

            var lines = File.ReadAllLines(_filePath);
            if (lines.Length == 0) return;

            Columns = lines[0].Split(',').Select(c => c.Trim()).ToList();
            ColumnsQuantity = Columns.Count;

            Page actualPage = new Page();

            for (int i = 1; i < lines.Length; i++)
            {
                var values = lines[i].Split(',');
                if (values.Length != ColumnsQuantity)
                {
                    Console.WriteLine($"Line {i + 1} has an incorrect number of columns.");
                    continue;
                }
                
                var tuple = new Tuple();
                for (int j = 0; j < ColumnsQuantity; j++)
                {
                    var value = values[j].Trim();

                    if (int.TryParse(value, out int intVal))
                    {
                        tuple.Cols[Columns[j]] = intVal;
                    }
                    else 
                    {
                        tuple.Cols[Columns[j]] = value;
                    }
                }

                if (!actualPage.AddTuple(tuple))
                {
                    Pages.Add(actualPage);
                    PageQuantity++;
                    actualPage = new Page();
                    actualPage.AddTuple(tuple);
                }
            }

            if (actualPage.OccupiedTuples > 0)
            {
                AddPage(actualPage);
            }
        }

        public void AddPage(Page page)
        {
            Pages.Add(page);
            PageQuantity++;
        }

        public List<Tuple> GetTuples()
        {
            var tuples = new List<Tuple>();
            foreach (var page in Pages)
            {
                tuples.AddRange(page.GetTuples());
            }
            return tuples;
        }

        public void SaveData() 
        {
            using (var writer = new StreamWriter(_filePath))
            {
                if (Columns.Count > 0)
                {
                    writer.WriteLine(string.Join(",", Columns));
                }

                foreach (var page in Pages)
                {
                    foreach (var tuple in page.GetTuples())
                    {
                        writer.WriteLine(tuple.ToCsvString(Columns));
                    }
                }
            }
        }
    }
}
