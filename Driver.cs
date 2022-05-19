using System.Runtime.InteropServices;
using System.Text;

namespace Sqldb;

public class Driver
{
    const int COLUMN_USERNAME_SIZE = 32;
    const int COLUMN_EMAIL_SIZE = 255;
    const int ID_SIZE = sizeof(uint);
    const int USERNAME_SIZE = sizeof(char) * COLUMN_USERNAME_SIZE;
    const int EMAIL_SIZE = sizeof(char) * COLUMN_EMAIL_SIZE;
    const int ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
    const int PAGE_SIZE = 4096;
    const int TABLE_MAX_PAGES = 100;
    const int ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
    const int TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
    const int ID_OFFSET = 0;
    const int USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
    const int EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

    Table table;

    public Driver()
    {
        table = new Table();
    }

    public MetaCommandResult do_meta_command(string input_string)
    {
        if (input_string == ".exit")
        {
            System.Environment.Exit(0);
        }
        return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
    }

    public unsafe PrepareResult prepare_statement(string input_string, out Statement statement)
    {
        statement = new Statement();
        if (input_string.StartsWith("insert"))
        {
            statement.statement_type = StatementType.STATEMENT_INSERT;
            var args = input_string.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (args.Length < 4)
                return PrepareResult.PREPARE_SYNTAX_ERROR;
            var userBytes = Encoding.Unicode.GetBytes(args[2].ToCharArray());
            var emailBytes = Encoding.Unicode.GetBytes(args[3].ToCharArray());
            statement.row_to_insert.id = UInt32.Parse(args[1]);
            fixed (
                char* ptrToUsername = statement.row_to_insert.username,
                ptrToEmail = statement.row_to_insert.email)
            {
                Marshal.Copy(userBytes, 0, (IntPtr) ptrToUsername, userBytes.Length);
                Marshal.Copy(emailBytes,0, (IntPtr) ptrToEmail, emailBytes.Length);
            }
            return PrepareResult.PREPARE_SUCCESS;
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

    Span<byte> row_slot(int row_num)
    {
        int page_num = row_num / ROWS_PER_PAGE;
        var page = table.pages[page_num];
        if (page is null)
        {
            page = table.pages[page_num] = new Memory<byte>(new byte[PAGE_SIZE]);
        }
        var row_offset = row_num % ROWS_PER_PAGE;
        var byte_offset = row_offset * ROW_SIZE;
        return page.Value.Slice(byte_offset).Span;
    }

    public class Statement
    {
        internal StatementType statement_type;
        internal Row row_to_insert;
    }

    public unsafe struct Row
    {
        public uint id;
        public fixed char username[COLUMN_USERNAME_SIZE];
        public fixed char email[COLUMN_EMAIL_SIZE];
    }

    struct Table
    {
        internal int num_rows;
        internal Memory<byte>?[] pages;
        public Table()
        {
            num_rows = 0;
            pages = new Memory<byte>?[TABLE_MAX_PAGES];
            for (int i = 0; i < TABLE_MAX_PAGES; i++)
            {
                pages[i] = null;
            }
        }
    }

    public enum MetaCommandResult
    {
        META_COMMAND_SUCCESS,
        META_COMMAND_UNRECOGNIZED_COMMAND
    }

    public enum PrepareResult
    {
        PREPARE_SUCCESS,
        PREPARE_SYNTAX_ERROR,
        PREPARE_UNRECOGNIZED_STATEMENT
    }

    public enum StatementType
    {
        STATEMENT_INSERT,
        STATEMENT_SELECT
    }

    public enum ExecuteResult
    {
        EXECUTE_SUCCESS,
        EXECUTE_TABLE_FULL
    }
}