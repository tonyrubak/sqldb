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

    public Memory<byte> GetPage(long page_num)
    {
        return pager.GetPage(page_num);
    }
}

class Cursor
{
    Table table;
    long row_num;
    public bool end_of_table;
    public enum CursorStartPosition
    {
        CursorStartBegin,
        CursorStartEnd,
    }

    public Cursor(Table table, CursorStartPosition start_position)
    {
        this.table = table;
        switch (start_position)
        {
            case CursorStartPosition.CursorStartBegin:
                row_num = 0;
                end_of_table = table.num_rows == row_num;
                break;
            case CursorStartPosition.CursorStartEnd:
                row_num = table.num_rows;
                end_of_table = true;
                break;
        }
    }

    public Span<byte> CursorValue()
    {
        long row_num = this.row_num;
        long page_num = row_num / Driver.ROWS_PER_PAGE;
        var page = this.table.GetPage(page_num);
        var row_offset = row_num % Driver.ROWS_PER_PAGE;
        var byte_offset = row_offset * Driver.ROW_SIZE;
        return page.Slice((int)byte_offset).Span;
    }

    public void Advance()
    {
        row_num += 1;
        if (row_num >= table.num_rows)
        {
            end_of_table = true;
        }
    }
}