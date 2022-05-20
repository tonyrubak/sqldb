namespace Sqldb;

class Table : IDisposable
{
    internal long num_rows;
    public Pager pager;
    public Table(string filename)
    {
        pager = Pager.Open(filename);
        num_rows = pager.file_length / Driver.ROW_SIZE;
    }

    public void Close()
    {
        pager.Close(num_rows);
        this.Dispose();
    }

    public void Dispose()
    {
        pager.Dispose();
    }
}
