// See https://aka.ms/new-console-template for more information
using System.Text;

var input_buffer = new StringBuilder();
while (true) {
    print_prompt();
    var input_string = read_input(input_buffer);

    if (input_string[0] == '.') {
        switch (do_meta_command(input_string)) {
            case MetaCommandResult.META_COMMAND_SUCCESS:
                continue;
            case MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
                Console.WriteLine("Unrecognized command '{0}'", input_string);
                continue;
        }
    }

    Statement statement;
    switch (prepare_statement(input_string, out statement)) {
        case PrepareResult.PREPARE_SUCCESS:
            break;
        case PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT:
            Console.WriteLine("Unrecognized keyword at start of '{0}'.",
                input_buffer.ToString());
            continue;
    }
    execute_statement(statement);
    Console.WriteLine("Executed");
}

void print_prompt() {
    Console.Write("db > ");
}

string read_input(StringBuilder input_buffer) {
    input_buffer.Clear();
    try {
        input_buffer.Append(Console.ReadLine());
    } catch {
        Console.WriteLine("Error reading input");
        System.Environment.Exit(-1);
    }
    return input_buffer.ToString();
}

MetaCommandResult do_meta_command(string input_string) {
    if (input_string == ".exit") {
        System.Environment.Exit(0);
    }
    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(string input_string, out Statement statement) {
    statement = new Statement();
    if (input_string.StartsWith("insert")) {
        statement.statement_type = StatementType.STATEMENT_INSERT;
        return PrepareResult.PREPARE_SUCCESS;
    } else if (input_string == "select") {
        statement.statement_type = StatementType.STATEMENT_SELECT;
        return PrepareResult.PREPARE_SUCCESS;
    } else {
        return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT;
    }
}

void execute_statement(Statement statement) {
    switch (statement.statement_type) {
        case StatementType.STATEMENT_INSERT:
            Console.WriteLine("This is where we would do an insert");
            break;
        case StatementType.STATEMENT_SELECT:
            Console.WriteLine("This is where we would do a select.");
            break;
    }
}

class Statement {
    public StatementType statement_type;
}

enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
}

enum PrepareResult {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
}

enum StatementType {
    STATEMENT_INSERT,
    STATEMENT_SELECT
}