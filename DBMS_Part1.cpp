#include <iostream>
#include <string.h>
#include <bits/stdc++.h>
#include <sys/types.h>
#include <sstream>

using namespace std;

//Macros and constants declarations
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
//Compact Row Declarations
class Row {
    public:
    uint32_t id;
    string username;
    string email;
};
const uint32_t ID_SIZE = sizeof(Row::id);
const uint32_t USERNAME_SIZE = sizeof(Row::username);
const uint32_t EMAIL_SIZE = sizeof(Row::email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
//Pages and table declarations
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROW_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROW_PER_PAGE * TABLE_MAX_PAGES;


//Enums Declarations
enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult {
    PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT, PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG, PREPARE_NEGATIVE_ID
};

enum StatementType {
    STATEMENT_INSERT, STATEMENT_SELECT
};

enum ExecuteResult {
    EXECUTE_SUCCESS, EXECUTE_TABLE_FULL
};

//Class Declarations
class InputBuffer {
    public:
    string buffer;
    size_t bufferLength;
    ssize_t inputLength;
};

class Statement {
    public:
    StatementType type;
    Row rowToInsert;
};

class Table {
    public:
    uint32_t numRows;
    void* pages[TABLE_MAX_PAGES];
};

//Methods Definitions
void NewInputBufferIntinialization(InputBuffer* inputBuffer) {
        inputBuffer->buffer = "";
        inputBuffer->bufferLength = 0;
        inputBuffer->inputLength = 0;
        return;
}

void PrintPrompt() {
    cout<< "db > ";
}

void ReadInput(InputBuffer* inputBuffer) {
    if(!getline(cin, inputBuffer->buffer, '\n')) {
        cerr << "Error reading input: " << strerror(errno) << endl;
        exit(EXIT_FAILURE);
    }
    inputBuffer->bufferLength = cin.gcount() - 1;
    inputBuffer->buffer[cin.gcount() - 1] = 0;  // remove trailing new line
}

void CloseInputBuffer(InputBuffer* inputBuffer) {
    delete &inputBuffer->buffer;
    free(inputBuffer);
}

MetaCommandResult DoMetaCommand(InputBuffer* inputBuffer) {
    if(inputBuffer->buffer == ".exit") {
        CloseInputBuffer(inputBuffer);
        return META_COMMAND_SUCCESS;
    }

    else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepareInsert(InputBuffer* inputBuffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    std::istringstream iss(inputBuffer->buffer);
    std::vector<std::string>  tokens;
    std::string token;

    while(iss >> token) {
        tokens.push_back(token);
    }

    if(tokens.size() < 4) {
        return PREPARE_SYNTAX_ERROR;
    }
    string keyword = tokens[0];
    string idString = tokens[1];
    string username = tokens[2];
    string email = tokens[3];
    int id = stoi(idString);

    if(id<0) {
        return PREPARE_NEGATIVE_ID;
    }
    if(username.length() > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if(email.length() > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->rowToInsert.id = id;
    statement->rowToInsert.username = username;
    statement->rowToInsert.email = email;

    return PREPARE_SUCCESS;

}

PrepareResult PrepareStatement(InputBuffer* inputBuffer, Statement* statement) {
    if(inputBuffer->buffer.compare(0, 6, "insert") == 0) {
        return prepareInsert(inputBuffer, statement);
    }
    if(inputBuffer->buffer.compare(0, 6, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void* RowSlot(Table* table, uint32_t rowNum) {
    uint32_t pageNum = rowNum / ROW_PER_PAGE;
    void* page = table->pages[pageNum];
    if(page == NULL) {
        page = table->pages[pageNum] = malloc(PAGE_SIZE);
    }
    uint32_t rowOffset = rowNum % ROW_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page + byteOffset;
}

void SerializeRow(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void DeserializeRow(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void PrintRow(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

ExecuteResult ExecuteInsert(Statement* statement, Table* table) {
    if(table->numRows >= TABLE_MAX_ROWS) {
        return EXECUTE_TABLE_FULL;
    }
    Row* rowToInsert = &(statement->rowToInsert);
    SerializeRow(rowToInsert, RowSlot(table, table->numRows));
    table->numRows++;
    return EXECUTE_SUCCESS;
}

ExecuteResult ExecuteSelect(Statement* statement, Table* table) {
    Row row;
    for(uint32_t i = 0; i < table->numRows; i++) {
        DeserializeRow(RowSlot(table,i), &row);
        PrintRow(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult ExecuteStatement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
        return ExecuteInsert(statement, table);
        case (STATEMENT_SELECT):
        return ExecuteSelect(statement, table);
        break;
    }
}

Table* NewTable() {
    Table* table = new Table();
    table->numRows = 0;
    for(uint32_t i = 0; i< TABLE_MAX_PAGES; i++) {
        table->pages[i] = NULL;
    }
    return table;
}

void FreeTable(Table* table) {
    for(int i=0; table->pages[i]; i++) {
        free(table->pages[i]);
    }
    free(table);
}

//MAIN FUNCTION
int main(int argc, char* argv[]) {
    Table* table = NewTable();
    InputBuffer* inputBuffer = new InputBuffer();
    NewInputBufferIntinialization(inputBuffer);

    while (true) {
        PrintPrompt();
        ReadInput(inputBuffer);

        if(inputBuffer->buffer[0] == '.') {
            switch(DoMetaCommand(inputBuffer)) {
                case (META_COMMAND_SUCCESS) :
                continue;

                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                printf("Unrecognized command '%s' \n", inputBuffer->buffer);
                continue;
            }
        }
        
        Statement statement;
        switch(PrepareStatement(inputBuffer, &statement)) {
            case (PREPARE_SUCCESS):
            break;

            case (PREPARE_SYNTAX_ERROR):
            printf("Syntax Error. Could not parse Statement.\n");
            continue;

            case (PREPARE_STRING_TOO_LONG):
            printf("String is too long.\n");
            continue;

            case (PREPARE_UNRECOGNIZED_STATEMENT):
            printf("Unrecognized keyword at start of '%s'.\n",
               inputBuffer->buffer);
            continue;

            case (PREPARE_NEGATIVE_ID):
            printf("ID must be positive.\n");
            continue;
        }
        switch (ExecuteStatement(&statement, table)) {
            case (EXECUTE_SUCCESS):
            printf("Executed.\n");
            break;

            case (EXECUTE_TABLE_FULL) :
                printf("Error: Table full.\n");
                break;
            
        }
    }
    return 0;
}