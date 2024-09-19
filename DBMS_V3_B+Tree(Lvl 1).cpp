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

//Common Node Header Layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + PARENT_POINTER_SIZE + IS_ROOT_SIZE;

//Leaf Node Header Layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

//leaf Node Body Layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1)/2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

//Internal Node Header Layout
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;

//Internal node body layout
const uint32_t INTERNAL_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;

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
    EXECUTE_SUCCESS, EXECUTE_TABLE_FULL, EXECUTE_DUPLICATE_KEY
};

enum NodeType {
    NODE_INTERNAL, NODE_LEAF
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
    uint32_t numPages;
    void* pages[TABLE_MAX_PAGES];
};

class Table {
    public:
    uint32_t rootPageNum;
    Pager* pager;
};

class Cursor {
    public:
    Table* table;
    uint32_t pageNum;
    uint32_t cellNum;
    bool endOfTable;
};

//Methods Definitions

bool IsNodeRoot(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void SetNodeRoot(void* node, bool isRoot) {
    uint8_t value = isRoot;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* LeafNodeNumCells(void* node) {
    return (uint32_t*) (node + LEAF_NODE_NUM_CELLS_OFFSET);
}

void* LeafNodeCell(void* node, uint32_t cellNum) {
    return node + LEAF_NODE_HEADER_SIZE + cellNum * LEAF_NODE_CELL_SIZE;
}

uint32_t* LeafNodeKey(void* node, uint32_t cellNum) {
    return (uint32_t*) (LeafNodeCell(node, cellNum));
}

void* LeafNodeValue(void* node, uint32_t cellNum) {
    return LeafNodeCell(node, cellNum) + LEAF_NODE_KEY_SIZE;
}

void SetNodeType(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

uint32_t* InternalNodeNumKeys(void* node) {
    return (uint32_t*)node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* InternalNodeRightChild(void* node) {
    return (uint32_t*)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* InternalNodeCell(void* node, uint32_t cellNum) {
    return (uint32_t*)node + INTERNAL_NODE_HEADER_SIZE + cellNum * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* InternalNodeChild(void* node, uint32_t childNum) {
    uint32_t numKeys = *InternalNodeNumKeys(node);
    if(childNum > numKeys) {
        printf("Tried to access child num %d > num keys %d.\n", childNum, numKeys);
        exit(EXIT_FAILURE);
    }
    else if(childNum == numKeys) {
        return InternalNodeRightChild(node);
    }
    else {
        return InternalNodeCell(node, childNum);
    }
}

uint32_t* InternalNodeKey(void* node, uint32_t keyNum) {
    return InternalNodeCell(node, keyNum) + INTERNAL_NODE_CHILD_SIZE;
}

uint32_t GetNodeMaxKey(void* node) {
    switch(GetNodeType(node)) {
        case NODE_INTERNAL:
        return *InternalNodeKey(node, *InternalNodeNumKeys(node) - 1); 
        case NODE_LEAF:
        return *LeafNodeKey(node, *LeafNodeNumCells(node) - 1);  
    }
}

void InitializeInternalNode(void* node) {
    SetNodeType(node, NODE_INTERNAL);
    SetNodeRoot(node, false);
    *InternalNodeNumKeys(node) = 0;
}

void CreatNewRoot(Table* table, uint32_t rightChildPageNum) {
    void* root = GetPage(table->pager, table->rootPageNum);
    void* rightChild = GetPage(table->pager, rightChildPageNum);
    uint32_t leftChildPageNum = GetUnusedPageNum(table->pager);
    void* leftChild = GetPage(table->pager, leftChildPageNum);
    memcpy(leftChild, root, PAGE_SIZE);
    SetNodeRoot(leftChild, false);
    InitializeInternalNode(root);
    SetNodeRoot(root, true);
    *InternalNodeNumKeys(root) = 1;
    *InternalNodeChild(root, 0) = leftChildPageNum;
    uint32_t leftChildMaxKey = GetNodeMaxKey(leftChild);
    *InternalNodeKey(root, 0) = leftChildMaxKey;
    *InternalNodeRightChild(root) = rightChildPageNum;
}

void InitializeLeafNode(void* node) {
    SetNodeType(node, NODE_LEAF);
    SetNodeRoot(node, false);
    *LeafNodeNumCells(node) = 0;
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
        if(pageNum >= pager->numPages) {
            pager->numPages = pageNum + 1;
        }
    }
    return pager->pages[pageNum];
}

Cursor* TableStart(Table* table) {
    Cursor* cursor = new Cursor();
    cursor->table = table;
    cursor->pageNum = table->rootPageNum;
    cursor->cellNum = 0;
    void* rootNode = GetPage(table->pager, table->rootPageNum);
    uint32_t numCells = *LeafNodeNumCells(rootNode);
    cursor->endOfTable = (numCells == 0);
    return cursor;
}

NodeType GetNodeType(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

Cursor* LeafNodeFind(Table* table, uint32_t pageNum, uint32_t key) {
    void* node = GetPage(table->pager, pageNum);
    uint32_t numCells = *LeafNodeNumCells(node);

    Cursor* cursor = new Cursor();
    cursor->table = table;
    cursor->pageNum = pageNum;

    //Binary Search
    uint32_t minIndex = 0;
    uint32_t onePastMinIndex = numCells;
    while(onePastMinIndex != minIndex) {
        uint32_t index = (minIndex + onePastMinIndex) / 2;
        uint32_t keyAtIndex = *LeafNodeKey(node, index);
        if(key == keyAtIndex) {
            cursor->cellNum = index;
            return cursor;
        }
        if(key < keyAtIndex) {
            onePastMinIndex = index;
        }
        else {
            minIndex = index + 1;
        }
    }

    cursor->cellNum = minIndex;
    return cursor;
}

Cursor* TableFind(Table* table, uint32_t key) {
    uint32_t rootPageNum = table->rootPageNum;
    void* rootNode = GetPage(table->pager, rootPageNum);

    if(GetNodeType(rootNode) == NODE_LEAF) {
        return LeafNodeFind(table, rootPageNum, key);
    }
    else {
        printf("Need to implement searching in ine=ternal nodes");
        exit(EXIT_FAILURE);
    }
}

void CursorAdvance(Cursor* cursor) {
    uint32_t pageNum = cursor->pageNum;
    void* node = GetPage(cursor->table->pager, pageNum);

    cursor->cellNum++;
    if(cursor->cellNum >= (*LeafNodeNumCells(node))) {
        cursor->endOfTable = true;
    }
}

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

void PagerFlush(Pager* pager, uint32_t pageNum) {
    if(pager->pages[pageNum] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);
    if(offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    ssize_t byteswritten = write(pager->fileDescriptor, pager->pages[pageNum], PAGE_SIZE);

    if(byteswritten == -1) {
        printf("Error writing in the file %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void DbClose(Table* table) {
        Pager* pager = table->pager;

        for(uint32_t i = 0; i< pager->numPages; i++) {
            if(pager->pages[i] == NULL) {
                continue;
            }
            PagerFlush(pager, i);
            free(pager->pages[i]);
            pager->pages[i] = NULL;
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

void* CursorValue(Cursor* cursor) {
    uint32_t pageNum = cursor->pageNum;
    void* page = GetPage(cursor->table->pager, pageNum);
    return LeafNodeValue(page, cursor->cellNum);
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

uint32_t GetUnusedPageNum(Pager* pager) {
    return pager->numPages;
}
void CreateNewRoot(Table* table, uint32_t right_child_page_num) {
/*
  Handle splitting the root.
  Old root copied to new page, becomes left child.
  Address of right child passed in.
  Re-initialize root page to contain the new root node.
  New root node points to two children.
  */

  void* root = GetPage(table->pager, table->rootPageNum);
  void* right_child = GetPage(table->pager, right_child_page_num);
  uint32_t left_child_page_num = GetUnusedPageNum(table->pager);
  void* left_child = GetPage(table->pager, left_child_page_num);
  /* Left child has data copied from old root */
  memcpy(left_child, root, PAGE_SIZE);
  SetNodeRoot(left_child, false);
  /* Root node is a new internal node with one key and two children */
  InitializeInternalNode(root);
  SetNodeRoot(root, true);
  *InternalNodeNumKeys(root) = 1;
  *InternalNodeChild(root, 0) = left_child_page_num;
  uint32_t left_child_max_key = GetNodeMaxKey(left_child);
  *InternalNodeKey(root, 0) = left_child_max_key;
  *InternalNodeRightChild(root) = right_child_page_num;
}

void LeafNodeSplitAndInsert(Cursor* cursor, uint32_t key, Row* value) {
    void* oldNode = GetPage(cursor->table->pager, cursor->pageNum);
    uint32_t newPageNum = GetUnusedPageNum(cursor->table->pager);
    void* newNode = GetPage(cursor->table->pager, newPageNum);
    InitializeLeafNode(newNode);

    for(int32_t i = LEAF_NODE_MAX_CELLS; i>=0; i++) {
        void* destinationNode;
        if(i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destinationNode = newNode;
        }
        else {
            destinationNode = oldNode;
        }
        uint32_t indexWithinNode = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = LeafNodeCell(destination, indexWithinNode);

        if(i == cursor->cellNum) {
            SerializeRow(value, destination);
        }
        else if(i > cursor->cellNum) {
            memcpy(destination, LeafNodeCell(oldNode, i-1), LEAF_NODE_CELL_SIZE);
        }
        else {
            memcpy(destination, LeafNodeCell(oldNode, i), LEAF_NODE_CELL_SIZE);
        }
    }

    *(LeafNodeNumCells(oldNode)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(LeafNodeNumCells(newNode)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    if(IsNodeRoot(oldNode)) {
        return CreateNewRoot(cursor->table, newPageNum);
    }
    else {
        printf("Need to implement update parent after split.\n");
        exit(EXIT_FAILURE);
    }
}

void LeafNodeInsert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = GetPage(cursor->table->pager, cursor->pageNum);
    uint32_t numCells = *LeafNodeNumCells(node);
    if(numCells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        LeafNodeSplitAndInsert(cursor, key, value);
        return;
    }
    if(cursor->cellNum < numCells) {
        // Make room for new cell
        for(uint32_t i = numCells; i> cursor->cellNum; i--) {
            memcpy(LeafNodeCell(node, i), LeafNodeCell(node, i- 1), LEAF_NODE_CELL_SIZE);
        }
    }
    *(LeafNodeNumCells(node)) += 1;
    *(LeafNodeKey(node, cursor->cellNum)) = key;
    SerializeRow(value, LeafNodeValue(node, cursor->cellNum));
}

ExecuteResult ExecuteInsert(Statement* statement, Table* table) {
    void* node = GetPage(table->pager, table->rootPageNum);
    uint32_t numCells = (*LeafNodeNumCells(node));
    Row* rowToInsert = &(statement->rowToInsert);
    uint32_t keyToInsert = rowToInsert->id;
    Cursor* cursor = TableFind(table, keyToInsert);

    if(cursor->cellNum < numCells) {
        uint32_t keyAtIndex = *LeafNodeKey(node, cursor->cellNum);
        if(keyAtIndex == keyToInsert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }


    LeafNodeInsert(cursor, rowToInsert->id, rowToInsert);
    return EXECUTE_SUCCESS;
}

ExecuteResult ExecuteSelect(Statement* statement, Table* table) {
    Cursor* cursor = TableStart(table);
    Row row;
    while(!(cursor->endOfTable)) {
        DeserializeRow(CursorValue(cursor), &row);
        PrintRow(&row);
        CursorAdvance(cursor);
    }
    free(cursor);
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
    pager->numPages = (fileLength / PAGE_SIZE);

    if(fileLength % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Table* DbOpen(const char* filename) {
    Pager* pager = PagerOpen(filename);
    Table* table = new Table();
    table->pager = pager;
    table->rootPageNum = 0;
    if(pager->numPages == 0) {
        // New Database. Initialize page 0 as leaf node too
        void* rootNode = GetPage(pager, 0);
        InitializeLeafNode(rootNode);
        SetNodeRoot(rootNode, true);
    }
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

            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
            case (EXECUTE_DUPLICATE_KEY):
            printf("Error: Duplicate Key.\n");
            break;
            
        }
    }
    return 0;
}
