using sgbg_trab_2.Operations;

public class JoinResult
{
    public string Name { get; }
    public Operator Op { get; }

    public JoinResult(string name, Operator op)
    {
        Name = name;
        Op = op;
    }
}
