using System.Runtime.InteropServices;
using System.Text;

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

public class Driver
{
    const int COLUMN_USERNAME_SIZE = 32;
    const int COLUMN_EMAIL_SIZE = 255;
    const int ID_SIZE = sizeof(uint);
    const int USERNAME_SIZE = sizeof(char) * COLUMN_USERNAME_SIZE;
    const int EMAIL_SIZE = sizeof(char) * COLUMN_EMAIL_SIZE;
    public const long ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
    public const int PAGE_SIZE = 4096;
    public const int TABLE_MAX_PAGES = 100;
    public const long ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
    const long TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
    const int ID_OFFSET = 0;
    const int USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
    const int EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

    Table table;

    public Driver(string filename)
    {
        table = new Table(filename);
    }

    public void Close()
    {
        table.Close();
    }

    public MetaCommandResult do_meta_command(string input_string)
    {
        if (input_string == ".exit")
        {
            table.Close();
            System.Environment.Exit(0);
        }
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
    }

    public PrepareResult prepare_statement(string input_string, out Statement statement)
    {
        statement = new Statement();
        if (input_string.StartsWith("insert"))
        {
            return prepare_insert(input_string, statement);
        }
        else if (input_string == "select")
        {
            statement.statement_type = StatementType.STATEMENT_SELECT;
            return PrepareResult.PREPARE_SUCCESS;
        }
        else
        {
            return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT;
        }
    }

    private static unsafe PrepareResult prepare_insert(string input_string, Statement statement)
    {
        statement.statement_type = StatementType.STATEMENT_INSERT;
        var args = input_string.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (args.Length < 4)
            return PrepareResult.PREPARE_SYNTAX_ERROR;
        string id_string = args[1];
        int id = Int32.Parse(id_string);
        if (id < 0)
        {
            return PrepareResult.PREPARE_NEGATIVE_ID;
        }
        string username = args[2];
        string email = args[3];
        if (id_string == String.Empty || username == String.Empty || email == String.Empty)
        {
            return PrepareResult.PREPARE_SYNTAX_ERROR;
        }
        if (username.Length > COLUMN_USERNAME_SIZE || email.Length > COLUMN_EMAIL_SIZE)
        {
            return PrepareResult.PREPARE_STRING_TOO_LONG;
        }
        var userBytes = Encoding.Unicode.GetBytes(username.ToCharArray());
        var emailBytes = Encoding.Unicode.GetBytes(email.ToCharArray());
        statement.row_to_insert.id = (uint)id;
        fixed (
            char* ptrToUsername = statement.row_to_insert.username,
            ptrToEmail = statement.row_to_insert.email)
        {
            Marshal.Copy(userBytes, 0, (IntPtr)ptrToUsername, userBytes.Length);
            Marshal.Copy(emailBytes, 0, (IntPtr)ptrToEmail, emailBytes.Length);
        }
        return PrepareResult.PREPARE_SUCCESS;
    }

    public ExecuteResult execute_statement(Statement statement)
    {
        switch (statement.statement_type)
        {
            case StatementType.STATEMENT_INSERT:
                return execute_insert(statement);
            case StatementType.STATEMENT_SELECT:
                return execute_select(statement);
            default:
                return ExecuteResult.EXECUTE_TABLE_FULL;
        }
    }

    ExecuteResult execute_insert(Statement statement)
    {
        if (table.num_rows >= TABLE_MAX_ROWS)
        {
            return ExecuteResult.EXECUTE_TABLE_FULL;
        }

        var row_to_insert = statement.row_to_insert;
        serialize_row(ref row_to_insert, row_slot(table.num_rows));
        table.num_rows++;
        return ExecuteResult.EXECUTE_SUCCESS;
    }

    ExecuteResult execute_select(Statement statement)
    {
        Row row;
        for (int i = 0; i < table.num_rows; i++)
        {
            row = deserialize_row(row_slot(i));
            print_row(row);
        }
        return ExecuteResult.EXECUTE_SUCCESS;
    }

    unsafe void print_row(Row row)
    {
        Console.WriteLine(
            "({0}, {1}, {2})",
            row.id,
            new String(row.username),
            new String(row.email));
    }

    void serialize_row(ref Row source, Span<byte> dest)
    {
        MemoryMarshal.Write<Row>(dest, ref source);
    }

    Row deserialize_row(Span<byte> source)
    {
        return MemoryMarshal.Read<Row>(source);
    }

    Span<byte> row_slot(long row_num)
    {
        long page_num = row_num / ROWS_PER_PAGE;
        var page = table.pager.GetPage(page_num);
        var row_offset = row_num % ROWS_PER_PAGE;
        var byte_offset = row_offset * ROW_SIZE;
        return page.Slice((int)byte_offset).Span;
    }

    public class Statement
    {
        internal StatementType statement_type;
        internal Row row_to_insert;
    }

    public unsafe struct Row
    {
        public uint id;
        public fixed char username[COLUMN_USERNAME_SIZE + 1];
        public fixed char email[COLUMN_EMAIL_SIZE + 1];
    }

    public enum MetaCommandResult
    {
        META_COMMAND_SUCCESS,
        META_COMMAND_UNRECOGNIZED_COMMAND,
    }

    public enum PrepareResult
    {
        PREPARE_NEGATIVE_ID,
        PREPARE_STRING_TOO_LONG,
        PREPARE_SUCCESS,
        PREPARE_SYNTAX_ERROR,
        PREPARE_UNRECOGNIZED_STATEMENT,
    }

    public enum StatementType
    {
        STATEMENT_INSERT,
        STATEMENT_SELECT,
    }

    public enum ExecuteResult
    {
        EXECUTE_SUCCESS,
        EXECUTE_TABLE_FULL,
    }
}