namespace Sqldb;

class Pager : IDisposable
{
    string filename;
    FileStream fs;
    public readonly long file_length;
    internal Memory<byte>?[] pages;

    public void Close(long num_rows)
    {
        long num_full_pages = num_rows / Driver.ROWS_PER_PAGE;
        for (int i = 0; i < num_full_pages; i++)
        {
            if (pages[i] is null)
            {
                continue;
            }
            Flush(i, Driver.PAGE_SIZE);
            pages[i] = null;
        }

        long num_addl_rows = num_rows % Driver.ROWS_PER_PAGE;
        if (num_addl_rows > 0)
        {
            int page_num = (int)num_full_pages;
            if (pages[page_num] is not null)
            {
                Flush(page_num, num_addl_rows * Driver.ROW_SIZE);
                pages[page_num] = null;
            }
        }
    }

    private void Flush(int page_num, long size)
    {
        if (pages[page_num] is null)
        {
            Console.WriteLine("Tried to flush null page");
            System.Environment.Exit(-1);
        }
        
        long offset = fs.Seek(page_num * Driver.PAGE_SIZE, SeekOrigin.Begin);
        try
        {
            fs.Write(pages[page_num].Value.Slice(0, (int)size).Span);
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error writing: {0}", ex.Message);
            System.Environment.Exit(-1);
        }
    }

    public Memory<byte> GetPage(long page_num)
    {
        if (page_num > Driver.TABLE_MAX_PAGES)
        {
            Console.WriteLine(
                "Tried to fetch page number out of bounds {0} > {1}",
                page_num,
                Driver.TABLE_MAX_PAGES);
            System.Environment.Exit(-1);
        }

        if (pages[page_num] is null)
        {
            var page_buffer = new byte[Driver.PAGE_SIZE];
            long num_pages = file_length / Driver.PAGE_SIZE;
            if (file_length % Driver.PAGE_SIZE != 0)
            {
                num_pages += 1;
            }

            if (page_num <= num_pages)
            {
                fs.Seek(page_num * Driver.PAGE_SIZE, SeekOrigin.Begin);
                try
                {
                    fs.Read(page_buffer, 0, Driver.PAGE_SIZE);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error reading file: {0}", ex.Message);
                    System.Environment.Exit(-1);
                }
            }
            pages[page_num] = new Memory<byte>(page_buffer);
        }
        return pages[page_num].Value;
    }

    public static Pager Open(string filename)
    {
        try
        {
            var fs = File.Open(
                filename,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite
            );
            long file_length = fs.Seek(0, SeekOrigin.End);
            Pager pager = new Pager(
                filename,
                fs,
                file_length,
                Driver.TABLE_MAX_PAGES);
            for (int i = 0; i < Driver.TABLE_MAX_PAGES; i++)
            {
                pager.pages[i] = null;
            }
            return pager;
        }
        catch (Exception ex)
        {
            Console.WriteLine("Unable to open file");
            Console.WriteLine(ex.Message);
            System.Environment.Exit(-1);
        }
        return null;
    }

    private Pager(
        string filename,
        FileStream fs,
        long file_length,
        int page_size)
    {
        this.filename = filename;
        this.fs = fs;
        this.file_length = file_length;
        this.pages = new Memory<byte>?[page_size];
    }

    public void Dispose()
    {
        this.Dispose(true);
    }

    protected void Dispose(bool disposing)
    {
        if (disposing)
        {
            GC.SuppressFinalize(this);
        }
        fs.Close();
    }

    ~Pager()
    {
        this.Dispose(false);
    }
}