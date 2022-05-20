namespace Sqldb;

class Table : IDisposable
{
    internal long num_rows;
    Pager pager;
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

    public Span<byte> row_slot(long row_num)
    {
        long page_num = row_num / Driver.ROWS_PER_PAGE;
        var page = pager.GetPage(page_num);
        var row_offset = row_num % Driver.ROWS_PER_PAGE;
        var byte_offset = row_offset * Driver.ROW_SIZE;
        return page.Slice((int)byte_offset).Span;
    }
}
