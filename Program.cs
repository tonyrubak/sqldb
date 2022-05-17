// See https://aka.ms/new-console-template for more information
using System.Text;

var input_buffer = new StringBuilder();
while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer[0] == '.') {
        switch (do_meta_command(input_buffer)) {
            case MetaCommandResult.META_COMMAND_SUCCESS:
                continue;
            case MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND:
                Console.WriteLine("Unrecognized command '{0}'", input_buffer.ToString());
                continue;
        }
    }

    Statement statement;
    switch (prepare_statement(input_buffer, out statement)) {
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

void read_input(StringBuilder input_buffer) {
    input_buffer.Clear();
    try {
        input_buffer.Append(Console.ReadLine());
    } catch {
        Console.WriteLine("Error reading input");
        System.Environment.Exit(-1);
    }
}

MetaCommandResult do_meta_command(StringBuilder input_buffer) {
    if (input_buffer.ToString() == ".exit") {
        System.Environment.Exit(0);
    }
    return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
}

PrepareResult prepare_statement(StringBuilder input_buffer, out Statement statement) {
    statement = new Statement();
    if (input_buffer.ToString().StartsWith("insert")) {
        statement.statement_type = StatementType.STATEMENT_INSERT;
        return PrepareResult.PREPARE_SUCCESS;
    } else if (input_buffer.ToString() == "select") {
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