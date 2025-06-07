using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace sgbg_trab_2.Models
{
    public class Page
    {
        public List<Tuple> Tuples { get; set; }
        public int OccupiedTuples { get; set; }

        public Page()
        {
            Tuples = new List<Tuple>();
            OccupiedTuples = 0;
        }

        public Page(List<Tuple> tuples)
        {
            Tuples = tuples;
            OccupiedTuples = tuples.Count;
        }

        public bool AddTuple(Tuple tuple)
        {
            if (OccupiedTuples < 10) 
            {
                Tuples.Add(tuple);
                OccupiedTuples++;
                return true;
            }
            return false;
        }

        public bool isFull () 
        {
            return OccupiedTuples >= 10;
        }

        public List<Tuple> GetTuples()
        {
            return new List<Tuple>(Tuples);
        }   
    }
}
