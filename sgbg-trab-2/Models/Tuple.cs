using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sgbg_trab_2.Models
{
    public class Tuple
    {
        public Dictionary<string, object> Cols { get; set; }

        public Tuple()
        {
            Cols = new Dictionary<string, object>();
        }

        public Tuple(Dictionary<string, object> cols)
        {
            Cols = cols;
        }

        public object GetValue(string columnName)
        {
            return Cols.ContainsKey(columnName) ? Cols[columnName] : null;
        }

        public string ToCsvString(List<string> columns)
        {
            var values = new List<string>();
            foreach (var col in columns)
            {
                var value = Cols.ContainsKey(col) ? Cols[col]?.ToString() ?? "" : "";
                values.Add(value);
            }
            return string.Join(",", values);
        }

        public override string ToString()
        {
            return string.Join(", ", Cols.Select(kv => $"{kv.Key}: {kv.Value}"));
        }
    }
}
