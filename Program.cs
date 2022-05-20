// See https://aka.ms/new-console-template for more information
using System.Text;


namespace Sqldb;

class Program
{
    static void Main(string[] args)
    {
        var input_buffer = new StringBuilder();
        string filename;

        if (args.Length > 0)
        {
            filename = args[0];
        }
        else
        {
            filename = "mydb.db";
        }

        var driver = new Driver(filename);

        while (true)
        {
            print_prompt();
            var input_string = read_input(input_buffer);

            if (input_string[0] == '.')
            {
                switch (driver.do_meta_command(input_string))
                {
                    case Driver.MetaCommandResult.META_COMMAND_SUCCESS:
                        continue;
                    case Driver.MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
                        Console.WriteLine("Unrecognized command '{0}'", input_string);
                        continue;
                }
            }

            Driver.Statement statement;
            switch (driver.prepare_statement(input_string, out statement))
            {
                case Driver.PrepareResult.PREPARE_SUCCESS:
                    break;
                case Driver.PrepareResult.PREPARE_SYNTAX_ERROR:
                    Console.WriteLine("Syntax error. Could not parse statement.");
                    continue;
                case Driver.PrepareResult.PREPARE_STRING_TOO_LONG:
                    Console.WriteLine("String too long.");
                    continue;
                case Driver.PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
                    Console.WriteLine("Unrecognized keyword at start of '{0}'.",
                        input_buffer.ToString());
                    continue;
            }
            switch (driver.execute_statement(statement))
            {
                case Driver.ExecuteResult.EXECUTE_SUCCESS:
                    Console.WriteLine("Executed.");
                    break;
                case Driver.ExecuteResult.EXECUTE_TABLE_FULL:
                    Console.WriteLine("Error: Table full.");
                    break;
            }
        }
    }
    static void print_prompt()
    {
        Console.Write("db > ");
    }

    static string read_input(StringBuilder input_buffer)
    {
        input_buffer.Clear();
        try
        {
            input_buffer.Append(Console.ReadLine());
        }
        catch
        {
            Console.WriteLine("Error reading input");
            System.Environment.Exit(-1);
        }
        return input_buffer.ToString();
    }
}