#include <iostream>
#include <string.h>
#include <bits/stdc++.h>
#include <sys/types.h>
#include <sstream>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

using namespace std;

//Macros and constants declarations
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define TABLE_MAX_PAGES 100
//Compact Row Declarations
class Row {
    public:
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
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

class Pager {
    public:
    int fileDescriptor;
    uint32_t fileLength;
    void* pages[TABLE_MAX_PAGES];
};

class Table {
    public:
    uint32_t numRows;
    Pager* pager;
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

void PagerFlush(Pager* pager, uint32_t pageNum, uint32_t size) {
    if(pager->pages[pageNum] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
    if(offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    ssize_t byteswritten = write(pager->fileDescriptor, pager->pages[pageNum], size);

    if(byteswritten == -1) {
        printf("Error writing in the file %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void DbClose(Table* table) {
    Pager* pager = table->pager;
    uint32_t numFullPages = table->numRows / ROW_PER_PAGE;

        for(uint32_t i = 0; i< numFullPages; i++) {
            if(pager->pages[i] == NULL) {
                continue;
            }
            PagerFlush(pager, i, PAGE_SIZE);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
        uint32_t numAdditionalRows = table->numRows % ROW_PER_PAGE;
        if(numAdditionalRows > 0) {
            uint32_t pageNum = numFullPages;
            if(pager->pages[pageNum] != NULL) {
                PagerFlush(pager, pageNum, numAdditionalRows * ROW_SIZE);
                free(pager->pages[pageNum]);
                pager->pages[pageNum] = NULL;
            }
        }
        int result = close(pager->fileDescriptor);
        if(result == -1) {
            printf("Error closing file\n");
            exit(EXIT_FAILURE);
        }
        for(uint32_t i = 0; i< TABLE_MAX_PAGES; i++) {
            void* page = pager->pages[i];
            if(page) {
                free(page);
                pager->pages[i] = NULL;
            }
        }
        free(pager);
        free(table);
}

MetaCommandResult DoMetaCommand(InputBuffer* inputBuffer, Table* table) {
    if(inputBuffer->buffer == ".exit") {
        DbClose(table);
        exit(EXIT_SUCCESS);
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
    strcpy(statement->rowToInsert.username, username.c_str());
    strcpy(statement->rowToInsert.email, email.c_str());

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

void* GetPage(Pager* pager, uint32_t pageNum) {
    if(pageNum > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number which is out of bounds %d > %d", pageNum, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    if(pager->pages[pageNum] == NULL) {
        //Cache miss. Allocate memory and load from the file.
        void* page = malloc(PAGE_SIZE);
        uint32_t numPages = pager->fileLength / PAGE_SIZE;
        if(pager->fileLength % PAGE_SIZE) {
            numPages++;
        }

        if(pageNum <= numPages) {
            lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
            ssize_t bytesRead = read(pager->fileDescriptor, page, PAGE_SIZE);
            if(bytesRead == -1) {
                printf("Error reading the file.\n");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[pageNum] = page;
    }
    return pager->pages[pageNum];
}

void* RowSlot(Table* table, uint32_t rowNum) {
    uint32_t pageNum = rowNum / ROW_PER_PAGE;
    void* page = GetPage(table->pager, pageNum);
    uint32_t rowOffset = rowNum % ROW_PER_PAGE;
    uint32_t byteOffset = rowOffset * ROW_SIZE;
    return page + byteOffset;
}

void SerializeRow(Row* source, void* destination) {

    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy((char*)(destination + USERNAME_OFFSET), source->username, USERNAME_SIZE);
    strncpy((char*)(destination + EMAIL_OFFSET), source->email, EMAIL_SIZE);
}

void DeserializeRow(void* source, Row* destination) {   
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy((destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy((destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void PrintRow(Row* row) {
    cout<< "(" << row->id << "," << row->username <<"," << row->email << ")\n";
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
    }
}

Pager* PagerOpen(const char* filename) {
    int fd = open(filename,
                O_RDWR |      // Read/Write mode
                    O_CREAT,  // Create file if it does not exist
                S_IWUSR |     // User write permission
                   S_IRUSR   // User read permission
               );
    if(fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t fileLength = lseek(fd, 0, SEEK_END);

    Pager* pager = new Pager();
    pager->fileDescriptor = fd;
    pager->fileLength = fileLength;

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Table* 
DbOpen(const char* filename) {
    Pager* pager = PagerOpen(filename);
    uint32_t numRows = pager->fileLength / ROW_SIZE;
    Table* table = new Table();
    table->pager = pager;
    table->numRows = numRows;
    return table;
}

//MAIN FUNCTION
int main(int argc, char* argv[]) {
    PrintPrompt();
    string filename;
    cin >> filename;
    cout << "\n";
    Table* table = DbOpen(filename.c_str());
    InputBuffer* inputBuffer = new InputBuffer();
    
    while (true) {
        PrintPrompt();
        ReadInput(inputBuffer);

        if(inputBuffer->buffer[0] == '.') {
            switch(DoMetaCommand(inputBuffer, table)) {
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
