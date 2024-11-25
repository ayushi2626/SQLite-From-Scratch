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
#define INVALID_PAGE_NUM UINT32_MAX
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
const uint32_t LEAF_NODE_PREV_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_PREV_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_PREV_LEAF_OFFSET + LEAF_NODE_PREV_LEAF_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE + LEAF_NODE_PREV_LEAF_SIZE;

//leaf Node Body Layout
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_SIZE + LEAF_NODE_KEY_OFFSET;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MIN_CELLS = 1;
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
const uint32_t INTERNAL_NODE_MIN_CELLS = 1;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;

//Enums Declarations
enum MetaCommandResult {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};

enum PrepareResult {
    PREPARE_SUCCESS, PREPARE_UNRECOGNIZED_STATEMENT, PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG, PREPARE_NEGATIVE_ID, PREPARE_SPACE
};

enum StatementType {
    STATEMENT_INSERT, STATEMENT_SELECT, STATEMENT_DELETE
};

enum ExecuteResult {
    EXECUTE_SUCCESS, EXECUTE_TABLE_FULL, EXECUTE_DUPLICATE_KEY, EXECUTE_NO_KEY
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
    uint32_t rowToDelete;
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

uint32_t* LeafNodePrevLeaf(void* node) {
    return (uint32_t*)(node + LEAF_NODE_PREV_LEAF_OFFSET);
}

uint32_t* LeafNodeNextLeaf(void* node) {
    return (uint32_t*)(node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

bool IsNodeRoot(void* node) {
    if(node == NULL) {
        return false;
    }
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

uint32_t* NodeParent(void* node) {
    return (uint32_t*) (node + PARENT_POINTER_OFFSET);
}

void SetNodeRoot(void* node, bool isRoot) {
    uint8_t value = isRoot;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
    *NodeParent(node) = NULL;
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

char* LeafNodeGetUsername(void* node, uint32_t index) {
    return (char*)(LeafNodeCell(node, index) + LEAF_NODE_VALUE_OFFSET + USERNAME_OFFSET);
}

char* LeafNodeGetEmail(void* node, uint32_t index) {
    return (char*)(LeafNodeCell(node, index) + LEAF_NODE_VALUE_OFFSET + EMAIL_OFFSET);
}

/*uint32_t LeafNodeGetId(void* node, uint32_t index) {
    return (uint32_t)(LeafNodeCell(node, index) + LEAF_NODE_VALUE_OFFSET + ID_OFFSET);
}*/

void SetNodeType(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

uint32_t* InternalNodeNumKeys(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}

uint32_t* InternalNodeRightChild(void* node) {
    return (uint32_t*)(node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}

uint32_t* InternalNodeCell(void* node, uint32_t cellNum) {
    return (uint32_t*)(node + INTERNAL_NODE_HEADER_SIZE + cellNum * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* InternalNodeChild(void* node, uint32_t childNum) {
    uint32_t numKeys = *InternalNodeNumKeys(node);
    if(childNum > numKeys) {
        printf("Tried to access child num %d > num keys %d.\n", childNum, numKeys);
        exit(EXIT_FAILURE);
    }
    else if(childNum == numKeys) {
        uint32_t* rightChild = InternalNodeRightChild(node);
        if(*rightChild == INVALID_PAGE_NUM) {
            printf("Tried to access a right child of node but was invalid page.\n");
            exit(EXIT_FAILURE);
        }
        return rightChild;
    }
    else {
        uint32_t* child = InternalNodeCell(node, childNum);
        if(*child == INVALID_PAGE_NUM) {
            printf("Tried to access child %d of node, but was invalid page\n");
            exit(EXIT_FAILURE);
        }
        return child;
    }
}

uint32_t* InternalNodeKey(void* node, uint32_t keyNum) {
    return (uint32_t*)((void*)InternalNodeCell(node, keyNum) + INTERNAL_NODE_CHILD_SIZE);
}

NodeType GetNodeType(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
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

uint32_t GetNodeMaxKey(Pager* pager, void* node) {
    if(GetNodeType(node) == NODE_LEAF) {
        return *LeafNodeKey(node, *LeafNodeNumCells(node) - 1);
    }
    void* rightChild = GetPage(pager, *InternalNodeRightChild(node));
    return GetNodeMaxKey(pager, rightChild);
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
    *InternalNodeRightChild(node) = INVALID_PAGE_NUM;
}

uint32_t GetUnusedPageNum(Pager* pager) {
    printf("Unused page number: %d\n", pager->numPages);
    return pager->numPages;
}

void InitializeLeafNode(void* node) {
    SetNodeType(node, NODE_LEAF);
    SetNodeRoot(node, false);
    *LeafNodeNumCells(node) = 0;
    *LeafNodeNextLeaf(node) = NULL;
    *LeafNodePrevLeaf(node) = NULL;
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

uint32_t InternalNodeFindChild(void* node, uint32_t key) {
    // Return the index of the child which should contain the given key
    uint32_t numKeys = *InternalNodeNumKeys(node);
    //Binary search
    uint32_t minIndex = 0;
    uint32_t maxIndex = numKeys; // there is one more child than key

    while (minIndex != maxIndex) {
    uint32_t index = (minIndex + maxIndex) / 2;
    uint32_t key_to_right = *InternalNodeKey(node, index);
    if (key_to_right > key) {
      maxIndex = index;
    } else {
      minIndex = index + 1;
    }
  }

  return minIndex;
}

Cursor* InternalNodeFind(Table* table, uint32_t pageNum, uint32_t key) {
    void* node = GetPage(table->pager, pageNum);
    uint32_t numKeys = *InternalNodeNumKeys(node);

    uint32_t childIndex = InternalNodeFindChild(node, key);
    uint32_t childNum = *InternalNodeChild(node, childIndex);
    void* child = GetPage(table->pager, childNum);
    switch(GetNodeType(child)) {
        case NODE_LEAF:
        return LeafNodeFind(table, childNum, key);
        case NODE_INTERNAL:
        return InternalNodeFind(table, childNum, key);
    }
}

Cursor* TableFind(Table* table, uint32_t key) {
    uint32_t rootPageNum = table->rootPageNum;
    void* rootNode = GetPage(table->pager, rootPageNum);

    if(GetNodeType(rootNode) == NODE_LEAF) {
        return LeafNodeFind(table, rootPageNum, key);
    }
    else {
        return InternalNodeFind(table, rootPageNum, key);
    }
}

Cursor* TableStart(Table* table) {
    Cursor* cursor = TableFind(table, 0);
    void* node = GetPage(table->pager, cursor->pageNum);
    uint32_t numCells = *LeafNodeNumCells(node);
    cursor->endOfTable = (numCells == 0);
}

void CursorAdvance(Cursor* cursor) {
    uint32_t pageNum = cursor->pageNum;
    void* node = GetPage(cursor->table->pager, pageNum);

    cursor->cellNum++;
    if(cursor->cellNum >= (*LeafNodeNumCells(node))) {
        uint32_t nextPageNum = *LeafNodeNextLeaf(node);
        if(nextPageNum == 0) {
            cursor->endOfTable = true;
        }
        else {
            cursor->pageNum = nextPageNum;
            cursor->cellNum = 0;
        }
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

PrepareResult PrepareDelete(InputBuffer* inputBuffer, Statement* statement) {
    statement->type = STATEMENT_DELETE;
    std::istringstream iss(inputBuffer->buffer);
    std::vector<std::string>  tokens;
    std::string token;

    while(iss >> token) {
        tokens.push_back(token);
    }

    if(tokens.size() < 2) {
        return PREPARE_SYNTAX_ERROR;
    }

    string keyword = tokens[0];
    string idString = tokens[1];
    uint32_t id = stoi(idString);
    if(id < 0) {
        return PREPARE_NEGATIVE_ID;
    }

    statement->rowToDelete = id;

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
    if(inputBuffer->buffer.compare(0, 6, "delete") == 0) {
        return PrepareDelete(inputBuffer, statement);
    }
    if(inputBuffer->buffer[0] == ' ' || inputBuffer->buffer[0] == '\n') {
        return PREPARE_SPACE;
    }
    else
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
    uint32_t id;
    memcpy(&id, (source + ID_OFFSET), ID_SIZE);
    memcpy((destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy((destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
    memcpy(&(destination->id), &id, ID_SIZE);
}

void PrintRow(Row* row) {
    cout<< "(" << row->id << "," << row->username <<"," << row->email << ")\n";
}

uint32_t GetNodeMinKey(void* node) {
    switch(GetNodeType(node)) {
        case NODE_INTERNAL:
        return *InternalNodeKey(node, 0); 
        case NODE_LEAF:
        return *LeafNodeKey(node, 0);  
    }
}

void CreateNewRoot(Table* table, uint32_t right_child_page_num) {
/*
  Handle splitting the root.
  Old root copied to new page, becomes left child.
  Address of right child passed in.
  Re-initialize root page to contain the new root node.
  New root node points to two children.
  */
 cout<<"Inside CreateNew Root\n";
  void* root = GetPage(table->pager, table->rootPageNum);
  void* right_child = GetPage(table->pager, right_child_page_num);
  uint32_t left_child_page_num = GetUnusedPageNum(table->pager);
  void* left_child = GetPage(table->pager, left_child_page_num);
  if(GetNodeType(root) == NODE_INTERNAL) {
    InitializeInternalNode(right_child);
    InitializeInternalNode(left_child);
  }
  /* Left child has data copied from old root */
  memcpy(left_child, root, PAGE_SIZE);
  SetNodeRoot(left_child, false);

  if(GetNodeType(left_child) == NODE_INTERNAL) {
    void* child;
    for(int i=0; i<*InternalNodeNumKeys(left_child); i++) {
        child = GetPage(table->pager, *InternalNodeChild(left_child, i));
        *NodeParent(child) = left_child_page_num;
    }
    child = GetPage(table->pager, *InternalNodeRightChild(left_child));
    *NodeParent(child) = left_child_page_num;
  }
  /* Root node is a new internal node with one key and two children */
  InitializeInternalNode(root);
  SetNodeRoot(root, true);
  *InternalNodeNumKeys(root) = 1;
  *InternalNodeChild(root, 0) = left_child_page_num;
  uint32_t right_child_min_key = GetNodeMinKey(right_child);
  *InternalNodeKey(root, 0) = right_child_min_key;
  *InternalNodeRightChild(root) = right_child_page_num;
  *NodeParent(left_child) = table->rootPageNum;
  *NodeParent(right_child) = table->rootPageNum;
  *LeafNodeNextLeaf(left_child) = right_child_page_num;
  *LeafNodePrevLeaf(right_child) = left_child_page_num;

  if(GetNodeType(right_child) == NODE_INTERNAL) {
    cout<<"here";
    if(*InternalNodeRightChild(right_child) == INVALID_PAGE_NUM) {
        cout<<"YESYES";
    }
  }
}

void UpdateInternalNodeKey(void* node, uint32_t oldKey, uint32_t newKey) {
    uint32_t oldChildIndex = InternalNodeFindChild(node, oldKey);
    cout<< "The index in UpdateInternalNodeKey is";
    cout<< oldChildIndex;
    *InternalNodeKey(node, oldChildIndex) = newKey;
}

void InternalNodeSplitAndInsert(Table* table, uint32_t parent_page_num,
                          uint32_t child_page_num);

void InternalNodeInsert(Table* table, uint32_t parentPageNum, uint32_t childPageNum) {
    /**
     * @brief Add a new child/key pair to parent that corresponds to child
     * 
     */
    cout<<"Inside InternalNodeInsert\n";
    cout<<"Getting parent";
    void* parent = GetPage(table->pager, parentPageNum);
    cout<<"getting child";
    void* child = GetPage(table->pager, childPageNum);
    cout<< "Gotten child";
    uint32_t childMinKey = GetNodeMinKey(child);
    uint32_t index = InternalNodeFindChild(parent, childMinKey);

    uint32_t originalNumKeys = *InternalNodeNumKeys(parent);

    if(originalNumKeys >= INTERNAL_NODE_MAX_CELLS) {
        cout<<"Entering InpternalNode Split an diNsert";
        InternalNodeSplitAndInsert(table, parentPageNum, childPageNum);
        return;
    }

    uint32_t rightChildPageNum = *InternalNodeRightChild(parent);
    // An internal node with a right child of INVALID_PAGE_NUM is empty

    cout<<rightChildPageNum;
    if(rightChildPageNum == INVALID_PAGE_NUM) {
        cout<<"Child min key";cout<<childMinKey;
        *InternalNodeRightChild(parent) = childPageNum;
        return;
    }
    cout<<"Geeting the right child";
    void* rightChild = GetPage(table->pager, rightChildPageNum);
    cout<<"get the righ tchild";
    *InternalNodeNumKeys(parent) = originalNumKeys + 1;

    if(childMinKey > GetNodeMaxKey(table->pager, rightChild)) {
        // Replace Right Child

        *InternalNodeChild(parent, originalNumKeys) = rightChildPageNum;
        *InternalNodeKey(parent, originalNumKeys) = childMinKey;
        *InternalNodeRightChild(parent) = childPageNum;
    }
    else {
        // Make room for the new cell
        for(uint32_t i = originalNumKeys; i> index; i--) {
            void* destination = InternalNodeCell(parent, i);
            void* source = InternalNodeCell(parent, i-1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *InternalNodeChild(parent, index) = childPageNum;
        *InternalNodeKey(parent, index) = childMinKey;
    }
}

void InternalNodeSplitAndInsert(Table* table, uint32_t parentPageNum, uint32_t childPageNum) {
    cout<<"Inside InternalNodeSplitAndInsert\n";
    uint32_t oldPageNum = parentPageNum;
    void* oldNode = GetPage(table->pager, parentPageNum);
    uint32_t oldMin = GetNodeMaxKey(oldNode);
    void* child = GetPage(table->pager, childPageNum);
    uint32_t childMin = GetNodeMinKey(child);
    uint32_t newPageNum = GetUnusedPageNum(table->pager);
    
    uint32_t splittingRoot = IsNodeRoot(oldNode);

    void* parent;
    void* newNode;
    if(splittingRoot) {
        CreateNewRoot(table, newPageNum);
        cout<<(*InternalNodeRightChild(GetPage(table->pager, newPageNum)));
        parent = GetPage(table->pager, table->rootPageNum);
        oldPageNum = *InternalNodeChild(parent, 0);
        oldNode = GetPage(table->pager, oldPageNum);
    }
    else {
        parent = GetPage(table->pager, *NodeParent(oldNode));
        newNode = GetPage(table->pager, newPageNum);
        InitializeInternalNode(newNode);
    }

    uint32_t* oldNumKeys = InternalNodeNumKeys(oldNode);
    uint32_t curPageNum = *InternalNodeRightChild(oldNode);
    cout<<"Right child needed";cout<<curPageNum;
    void* cur = GetPage(table->pager, curPageNum);
    InternalNodeInsert(table, newPageNum, curPageNum);
    *NodeParent(cur) = newPageNum;
    *InternalNodeRightChild(oldNode) = INVALID_PAGE_NUM;

    for(int i= INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS / 2; i--) {
        curPageNum = *InternalNodeChild(oldNode, i);
        cur = GetPage(table->pager, curPageNum);

        InternalNodeInsert(table, newPageNum, curPageNum);
        *NodeParent(cur) = newPageNum;
        (*oldNumKeys)--; 
    }
    cout<<"Old node(left child now) has %d", *oldNumKeys;
    *InternalNodeRightChild(oldNode) = *InternalNodeChild(oldNode, *oldNumKeys - 1);
    uint32_t maxAfterSplit = GetNodeMaxKey(oldNode);
    uint32_t destinationPageNum = childMin < maxAfterSplit ? oldPageNum : newPageNum;
    InternalNodeInsert(table, destinationPageNum, childPageNum);
    *NodeParent(child) = destinationPageNum;
    UpdateInternalNodeKey(parent, oldMin, GetNodeMinKey(newNode));
    if(!splittingRoot) {
        InternalNodeInsert(table, *NodeParent(oldNode), newPageNum);
        *NodeParent(newNode) = *NodeParent(oldNode);
    }

}

void LeafNodeSplitAndInsert(Cursor* cursor, uint32_t key, Row* value) {
    cout<< "Inside Leaf Node to insert\n";
    void* oldNode = GetPage(cursor->table->pager, cursor->pageNum);
    uint32_t oldMin = GetNodeMaxKey(oldNode);
    uint32_t newPageNum = GetUnusedPageNum(cursor->table->pager);
    void* newNode = GetPage(cursor->table->pager, newPageNum);
    InitializeLeafNode(newNode);
    *NodeParent(newNode) = *NodeParent(oldNode);
    *LeafNodeNextLeaf(newNode) = *LeafNodeNextLeaf(oldNode);
    *LeafNodePrevLeaf(newNode) = cursor->pageNum;
    *LeafNodeNextLeaf(oldNode) = newPageNum;
    *LeafNodePrevLeaf(LeafNodeNextLeaf(newNode)) = newPageNum;

    for(int32_t i = LEAF_NODE_MAX_CELLS; i>=0; i--) {
        void* destinationNode;
        if(i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destinationNode = newNode;
        }
        else {
            destinationNode = oldNode;
        }
        uint32_t indexWithinNode = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = LeafNodeCell(destinationNode, indexWithinNode);

        if(i == cursor->cellNum) {
            SerializeRow(value, LeafNodeValue(destinationNode, indexWithinNode));
            *LeafNodeKey(destinationNode, indexWithinNode) = key;
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
        uint32_t parentPageNum = *NodeParent(oldNode);
        uint32_t newMin = GetNodeMinKey(newNode);
        void* parent = GetPage(cursor->table->pager, parentPageNum);
        cout<< oldMin;
        cout<<'\n';
        cout<< newMin;
        UpdateInternalNodeKey(parent, oldMin, newMin);
        InternalNodeInsert(cursor->table, parentPageNum, newPageNum);
        return;
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

void InternalNodeDelete(Cursor* cursor, uint32_t rowIdToDelete) {
    void * node = GetPage(cursor->table->pager, cursor->pageNum);
    int numOfCells = *InternalNodeNumKeys(node);
    int indexOfKey = -1;

    for(int i=0; i<numOfCells; i++) {
        if(*InternalNodeKey(node, i) == rowIdToDelete) {
            indexOfKey = i;
            break;
        }
    }
    if(indexOfKey != -1) {
        void* child = GetPage(cursor->table->pager, *InternalNodeChild(node, indexOfKey + 1));

        while(GetNodeType(child) != NODE_LEAF) {
            child = GetPage(cursor->table->pager, *InternalNodeChild(node, 0));
        }
        if(*LeafNodeNumCells(child) == 0) {
            *InternalNodeNumKeys(node) = *InternalNodeNumKeys(node) - 1;
            return;
        }
        *InternalNodeKey(node, indexOfKey) = *LeafNodeKey(child, 0);
    }
}

void LeafNodeDelete(Cursor* cursor, uint32_t rowIdToDelete) {   
    void * node = GetPage(cursor->table->pager, cursor->pageNum);
    uint32_t numOfCells = *LeafNodeNumCells(node);
    for(int i = cursor->cellNum + 1 ; i < numOfCells; i++) {
        memcpy(LeafNodeCell(node, i-1), LeafNodeCell(node, i), LEAF_NODE_CELL_SIZE);
    }
    *LeafNodeNumCells(node) = numOfCells - 1;
    if(*NodeParent(node) != NULL) {
        void* parent = GetPage(cursor->table->pager, *NodeParent(node));

        //If the node is the rightmost child of the parent
        if(*InternalNodeRightChild(parent) == rowIdToDelete) {
            *InternalNodeKey(parent, *InternalNodeNumKeys(parent) - 1) = *LeafNodeKey(node, 0);
            return;
        }

        //If the child isn't the rightmost child we run a loop
        int indexOfKeyInParent = 0;
        for(int i = 0; i <*InternalNodeNumKeys(parent); i++) {
            if(*InternalNodeKey(node, i) > rowIdToDelete) {
                indexOfKeyInParent = i;
                break;
            }
        }
         
        if(indexOfKeyInParent) {
            *InternalNodeKey(node, indexOfKeyInParent) = *LeafNodeKey(node, 0);
        }
    }
}

void BorrowFromRightSiblingLeaf(Cursor* cursor, uint32_t rowIdToDelete, void* node, uint32_t nextNode) {
    void* next = GetPage(cursor->table->pager, nextNode);
    int numOfCells = *LeafNodeNumCells(node);
    uint32_t key = *LeafNodeKey(next, 0);
    Row rowToShift;
    DeserializeRow(LeafNodeValue(next, 0), &rowToShift);
    SerializeRow(&rowToShift, LeafNodeValue(node, numOfCells));
    *LeafNodeKey(node, numOfCells) = *LeafNodeKey(next, 0);
    for(int i=1; i<*LeafNodeNumCells(next); i++) {
        Row row;
        DeserializeRow(LeafNodeValue(next, i), &row);
        SerializeRow(&row, LeafNodeValue(next, i - 1));
        *LeafNodeKey(next, i - 1) = *LeafNodeKey(next, i);
    }
   
    *LeafNodeNumCells(node) = numOfCells + 1;
    *LeafNodeNumCells(next) = *LeafNodeNumCells(next) - 1;
    void* parent = GetPage(cursor->table->pager, *NodeParent(next));

    //We check the right child which is stored seperately in internal node
    if(*InternalNodeRightChild(parent) == nextNode) {
        *InternalNodeKey(parent, *InternalNodeNumKeys(parent) - 1) = *LeafNodeKey(next, 0);
        return;
    }

    //Then we loop over the keys if the above condition is not satisfied
    for(int i=1; i<*InternalNodeNumKeys(parent) - 1; i++) {
        if(*InternalNodeChild(parent, i) == nextNode) {
            *InternalNodeKey(parent, i - 1) = *LeafNodeKey(next, 0);
            break;
        }
    }
}

void BorrowFromLeftSiblingLeaf(Cursor* cursor, uint32_t rowIdToDelete, void* node, uint32_t prevNode) {
    void* prev = GetPage(cursor->table->pager, prevNode);
    int numOfCells = *LeafNodeNumCells(node);
    uint32_t key = *LeafNodeKey(prev, *LeafNodeNumCells(prev) - 1);
    Row rowToShift;
    DeserializeRow(LeafNodeValue(prev, *LeafNodeNumCells(prev) - 1), &rowToShift);
    for(int i=0; i<*LeafNodeNumCells(node); i++) {
        Row row;
        DeserializeRow(LeafNodeValue(node, i), &row);
        SerializeRow(&row, LeafNodeValue(node, i + 1));
        *LeafNodeKey(node, i + 1) = *LeafNodeKey(node, i);
    }
    SerializeRow(&rowToShift, LeafNodeValue(node, 0));
    *LeafNodeKey(node, 0) = key;
    *LeafNodeNumCells(node) = numOfCells + 1;
    *LeafNodeNumCells(prev) = *LeafNodeNumCells(prev) - 1;
    void* parent = GetPage(cursor->table->pager, *NodeParent(prev));

    //If the node is rightmost child of the parent
    if(*InternalNodeRightChild(parent) == cursor->pageNum) {
        *InternalNodeKey(parent, *InternalNodeNumKeys(parent) - 1) = *LeafNodeKey(node, 0);
        *LeafNodeNumCells(node) = numOfCells + 1;
        *LeafNodeNumCells(prev) = *LeafNodeNumCells(prev) - 1;
        return;
    }

    //If the node is not the rightmost child
    for(int i=1; i<*InternalNodeNumKeys(parent) - 1; i++) {
        if(*InternalNodeChild(parent, i) == cursor->pageNum) {
            *InternalNodeKey(node, i -1) = *LeafNodeKey(node, 0);
            break;
        }
    }
}

void MergeSiblingLeftLeaf(Cursor* cursor, uint32_t rowIdToDelete, void* node, uint32_t prevNode) {
    void* prev = GetPage(cursor->table->pager, prevNode);
    int numOfCells = *LeafNodeNumCells(node);
    void* parent = GetPage(cursor->table->pager, *NodeParent(prev));
    
    int index = *LeafNodeNumCells(prev);
    for(int i=0; i<*LeafNodeNumCells(node); i++) {
        Row row;
        DeserializeRow(LeafNodeValue(node, i), &row);
        SerializeRow(&row, LeafNodeValue(prev, index));
        *LeafNodeKey(prev, index) = *LeafNodeKey(node, i);
        index++;
    }
    *LeafNodeNumCells(prev) = *LeafNodeNumCells(prev) + *LeafNodeNumCells(node);

    if(*InternalNodeRightChild(parent) == cursor->pageNum) {
        *InternalNodeRightChild(parent) = prevNode;
        *InternalNodeNumKeys(parent) = *InternalNodeNumKeys(parent) - 1;
        return;
    }
    int shiftingInParent;
    for(int i=0;i<*InternalNodeNumKeys(parent); i++) {
        if(*InternalNodeChild(parent, i) == cursor->pageNum) {
            shiftingInParent = i;
            break;
        }
    }
    for(int i= shiftingInParent + 1; i<*InternalNodeNumKeys(parent); i++) {
        *InternalNodeKey(parent, i - 1) = *InternalNodeKey(parent, i);
    }
    *InternalNodeNumKeys(parent) = *InternalNodeNumKeys(parent) - 1;
}

void MergeSiblingRightLeaf(Cursor* cursor, uint32_t rowIdToDelete, void* node, uint32_t nextNode) {
    void* next = GetPage(cursor->table->pager, nextNode);
    void* parent = GetPage(cursor->table->pager, *NodeParent(next));
    int index = *LeafNodeNumCells(node);
    for(int i=0; i<*LeafNodeNumCells(next);i++) {
        Row row;
        DeserializeRow(LeafNodeValue(next, i), &row);
        SerializeRow(&row, LeafNodeValue(node, index));
        *LeafNodeKey(node, index) = *LeafNodeKey(next, i);
        index++;
    }
    *LeafNodeNumCells(node) = *LeafNodeNumCells(node) + *LeafNodeNumCells(next);

    if(*InternalNodeRightChild(parent) == nextNode) {
        *InternalNodeRightChild(parent) = cursor->pageNum;
        *InternalNodeNumKeys(parent) = *InternalNodeNumKeys(parent) - 1;
        return;
    }

    int shiftingInParent;
    for(int i=0;i<*InternalNodeNumKeys(parent); i++) {
        if(*InternalNodeChild(parent, i) == nextNode) {
            shiftingInParent = i;
            break;
        }
    }
    
    for(int i=shiftingInParent+1; i<*InternalNodeNumKeys(parent); i++) {
        *InternalNodeKey(parent, i - 1) = *InternalNodeKey(parent, i);
    }

    *InternalNodeNumKeys(parent) = *InternalNodeNumKeys(parent) - 1;    
}

void BorrowFromRightSiblingInternal(Cursor* cursor, int indexInParent, void* node, void* next) {
     
    void* parent = GetPage(cursor->table->pager, *NodeParent(next));
    //Inserting the new key in node
    *InternalNodeKey(node, *InternalNodeNumKeys(node)) = *InternalNodeKey(parent, indexInParent);
    *InternalNodeNumKeys(node) = *InternalNodeNumKeys(node) + 1;

    //Changing the parent key
    *InternalNodeKey(parent, indexInParent) = *InternalNodeKey(next, 0);

    //Insert the new child in node
    uint32_t childFirst = *InternalNodeChild(next, 0);
    uint32_t rightmostChild = *InternalNodeRightChild(node);
    *InternalNodeChild(node, *InternalNodeNumKeys(node) - 1) = rightmostChild;
    *InternalNodeRightChild(node) = childFirst;

    //Delete the first key and child of next
    for(int i=1; i<*InternalNodeNumKeys(next); i++) {
        *InternalNodeKey(next, i-1) = *InternalNodeKey(next, i);
        *InternalNodeChild(next, i - 1) = *InternalNodeChild(next, i);
    }
    *InternalNodeNumKeys(next) = *InternalNodeNumKeys(next) - 1;
}

void BorrowFromLeftSiblingInternal(Cursor* cursor, int indexInParent, void* node, void* prev) {
    void* parent = GetPage(cursor->table->pager, *NodeParent(prev));

    //Made space for the new key 
    for(int i=0; i<*InternalNodeNumKeys(node); i++) {
        *InternalNodeKey(node, i+1) = *InternalNodeKey(node, i);
        *InternalNodeChild(node, i+1) = *InternalNodeChild(node, i); 
    }

    //Copied the new key into node
    *InternalNodeKey(node, 0) = *InternalNodeKey(parent, indexInParent - 1);

    //Change the parent key to the new one
    *InternalNodeKey(parent, indexInParent - 1) = *InternalNodeKey(prev, *InternalNodeNumKeys(prev) - 1);

    uint32_t prevRightMostChild = *InternalNodeRightChild(prev);
    *InternalNodeChild(node, 0) = prevRightMostChild;

    //Delete the last key and righmost child from prev
    uint32_t newRightmostChildPRev = *InternalNodeChild(prev, *InternalNodeNumKeys(prev) - 1);
    *InternalNodeRightChild(prev) = *InternalNodeChild(prev, *InternalNodeNumKeys(prev) - 1);
    *InternalNodeNumKeys(prev) = *InternalNodeNumKeys(prev) - 1;
}

void MergeSiblingRightInternal(Cursor* cursor, int indexInParent, void* node, void* next) {
    void* parent = GetPage(cursor->table->pager, *NodeParent(next));

    //Insert the new key in the node
    *InternalNodeKey(node, *InternalNodeNumKeys(node)) = *InternalNodeKey(parent, indexInParent);
    *InternalNodeNumKeys(node) = *InternalNodeNumKeys(node) + 1;

    //Delete the key and it's right child from parent
    if(indexInParent == *InternalNodeNumKeys(parent) - 1) {
        *InternalNodeRightChild(parent) = *InternalNodeChild(parent, *InternalNodeNumKeys(parent) - 1);
        *InternalNodeNumKeys(parent) = *InternalNodeNumKeys(parent) - 1;
    }
    else{
        for(int i=indexInParent + 1; i<*InternalNodeNumKeys(parent); i++) {
            *InternalNodeKey(parent, i - 1) = *InternalNodeKey(parent, i);
        }
        *InternalNodeChild(parent, indexInParent) = *InternalNodeChild(parent, indexInParent + 1);
        *InternalNodeNumKeys(parent) = *InternalNodeNumKeys(parent) - 1;
    }

    //Insert next node's keys and child in node
    *InternalNodeChild(node, *InternalNodeNumKeys(node) - 1) = *InternalNodeRightChild(node);

    int index = *InternalNodeNumKeys(node);
    for(int i=0; i<*InternalNodeNumKeys(next); i++) {
        *InternalNodeKey(node, index) = *InternalNodeKey(next, i);
        *InternalNodeChild(node, index) = *InternalNodeChild(next, i);
        index++;
    }
    *InternalNodeRightChild(node) = *InternalNodeRightChild(next);
    *InternalNodeNumKeys(node) = *InternalNodeNumKeys(node) + *InternalNodeNumKeys(next);
}

void MergeSiblingLeftInternal(Cursor* cursor, int indexInParent, void* node, void* prev) {
    void* parent = GetPage(cursor->table->pager, *NodeParent(prev));

    //Insert new key in prev
    *InternalNodeKey(prev, *InternalNodeNumKeys(prev)) = *InternalNodeKey(parent, indexInParent - 1);
    *InternalNodeNumKeys(prev) = *InternalNodeNumKeys(prev) + 1;

    //Delete a key and child from parent
    for(int i=indexInParent;i<*InternalNodeNumKeys(parent); i++) {
        *InternalNodeKey(parent, i-1) = *InternalNodeKey(parent, i);
    }
    *InternalNodeNumKeys(parent) = *InternalNodeNumKeys(parent) - 1;

    //Insert all keys and value of node into prev
    *InternalNodeChild(prev, *InternalNodeNumKeys(prev) - 1) = *InternalNodeRightChild(prev);
    int index = *InternalNodeNumKeys(prev);
    for(int i=0;i<*InternalNodeNumKeys(node); i++) {
        *InternalNodeKey(prev, index) = *InternalNodeKey(node, i);
        *InternalNodeChild(prev, index) = *InternalNodeChild(node, i);
    }
    *InternalNodeRightChild(prev) = *InternalNodeRightChild(node);
    *InternalNodeNumKeys(prev) = *InternalNodeNumKeys(prev) + *InternalNodeNumKeys(node);
}

void NodeDelete(Cursor* cursor, uint32_t rowIdToDelete) {
     
    void* node = GetPage(cursor->table->pager, cursor->pageNum);
    int sizeOfNode;
    bool minCapacityInNode;
    if(GetNodeType(node) == NODE_LEAF)  {
        LeafNodeDelete(cursor, rowIdToDelete);
        sizeOfNode = *LeafNodeNumCells(node);
        minCapacityInNode = sizeOfNode < LEAF_NODE_MIN_CELLS ? true : false;
    }
    else{
        InternalNodeDelete(cursor, rowIdToDelete);
        sizeOfNode = *InternalNodeNumKeys(node);
        minCapacityInNode = sizeOfNode < INTERNAL_NODE_MIN_CELLS ? true : false;
    }

    if(minCapacityInNode) {
        if(GetNodeType(node) == NODE_LEAF && !(IsNodeRoot(node))) {
           
            uint32_t prev = *LeafNodePrevLeaf(node);
            uint32_t next = *LeafNodeNextLeaf(node);
            void* prevNode = NULL;
            void* nextNode = NULL;

    
            if(prev) {
            prevNode = GetPage(cursor->table->pager, prev);
            }
            if(next) {
            nextNode = GetPage(cursor->table->pager, next);
            
            }

            if(next && *NodeParent(nextNode) == *NodeParent(node) && *LeafNodeNumCells(nextNode) > LEAF_NODE_MIN_CELLS) {
                BorrowFromRightSiblingLeaf(cursor, rowIdToDelete, node, next);
            }

            if(prev && *NodeParent(prevNode) == *NodeParent(node) && *LeafNodeNumCells(prevNode) > LEAF_NODE_MIN_CELLS) {
              
                BorrowFromLeftSiblingLeaf(cursor, rowIdToDelete, node, prev);
            }

            if(next && *NodeParent(nextNode) == *NodeParent(node) && *LeafNodeNumCells(nextNode) <= LEAF_NODE_MIN_CELLS) {
             
                MergeSiblingRightLeaf(cursor, rowIdToDelete, node, next);
            }

            if(prev && *NodeParent(prevNode) == *NodeParent(node) && *LeafNodeNumCells(prevNode) <= LEAF_NODE_MIN_CELLS) {
              
                MergeSiblingLeftLeaf(cursor, rowIdToDelete, node, next);
            }
        }
        else if(IsNodeRoot(node)) {
            if(*InternalNodeNumKeys(node) == 0) {
                uint32_t child = *InternalNodeRightChild(node);
                cursor->table->rootPageNum = child;
                void* root = GetPage(cursor->table->pager, cursor->table->rootPageNum);
                SetNodeRoot(root, true);
                SetNodeType(root, NODE_LEAF);
                *LeafNodeNextLeaf(root) = NULL;
                *LeafNodePrevLeaf(root) = NULL;
            }
            return;
        }
        else {
            if(*InternalNodeRightChild(node) == cursor->pageNum) {
                void* parent = GetPage(cursor->table->pager, *NodeParent(node));
                void* prev = GetPage(cursor->table->pager,*InternalNodeChild(parent, *InternalNodeNumKeys(parent)- 1));
                if(prev && NodeParent(node) == NodeParent(prev) && *InternalNodeNumKeys(prev) > INTERNAL_NODE_MIN_CELLS) {
                    BorrowFromLeftSiblingInternal(cursor, rowIdToDelete, node, prev);
                }
                if(prev && NodeParent(node) == NodeParent(prev) && *InternalNodeNumKeys(prev) <= INTERNAL_NODE_MIN_CELLS) {
                    MergeSiblingLeftInternal(cursor, rowIdToDelete, node, prev);
                }
            }
            else {
                int indexInParent = -1;
                void* parent = GetPage(cursor->table->pager, *NodeParent(node));
                for(int i=0;i<*InternalNodeNumKeys(parent); i++) {
                    if(*InternalNodeChild(node, i) == cursor->pageNum) {
                        indexInParent = i;
                        break;
                    }
                }
                void* next = NULL;
                void* prev = NULL;
                if(*InternalNodeNumKeys(parent) > indexInParent + 1) {
                    next = GetPage(cursor->table->pager, *InternalNodeChild(node, indexInParent + 1));
                }
                if(indexInParent) {
                    prev = GetPage(cursor->table->pager, *InternalNodeChild(node, indexInParent - 1));
                }

                if(next && NodeParent(next) == NodeParent(node) && *InternalNodeNumKeys(next) > INTERNAL_NODE_MIN_CELLS) {
                    BorrowFromRightSiblingInternal(cursor, indexInParent, node, next);

                }
                else if(prev && NodeParent(prev) == NodeParent(node) && *InternalNodeNumKeys(prev) > INTERNAL_NODE_MIN_CELLS) {
                    BorrowFromLeftSiblingInternal(cursor, rowIdToDelete, node, prev);
                }
                else if(next && NodeParent(next) == NodeParent(node) && *InternalNodeNumKeys(next) <= INTERNAL_NODE_MIN_CELLS) {
                    MergeSiblingRightInternal(cursor, rowIdToDelete, node, next);
                }

                else if(prev && NodeParent(prev) == NodeParent(node) && *InternalNodeNumKeys(prev) <= INTERNAL_NODE_MIN_CELLS) {
                    MergeSiblingLeftInternal(cursor, rowIdToDelete, node, prev);
                }

            }
        }
    }
    if(IsNodeRoot(node)) {
        return;
    }
    if(*NodeParent(node) != NULL || IsNodeRoot(GetPage(cursor->table->pager, *NodeParent(node)))) {
        //Delete the root from the parent recursively
        Cursor* cursorNew = new Cursor();
        cursorNew->pageNum = *NodeParent(node);
        cursorNew->table = cursor->table;
        cursorNew->cellNum = NULL;
        NodeDelete(cursorNew, rowIdToDelete);
    }
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

ExecuteResult ExecuteDelete(Statement* statement, Table* table) {
    uint32_t rowIdToDelete = statement->rowToDelete;
    void* node = GetPage(table->pager, table->rootPageNum);
    uint32_t numOfCells = *(LeafNodeNumCells(node));
    Cursor* cursor = TableFind(table, rowIdToDelete);

    NodeDelete(cursor, rowIdToDelete);
    return EXECUTE_SUCCESS;
}

ExecuteResult ExecuteStatement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
        return ExecuteInsert(statement, table);
        case (STATEMENT_SELECT):
        return ExecuteSelect(statement, table);
        case (STATEMENT_DELETE):
        return ExecuteDelete(statement, table);
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
        //exit(EXIT_FAILURE);
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

void Indent(uint32_t level) {
  for (uint32_t i = 0; i < level; i++) {
    printf("  ");
  }
}

void PrintTree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
  void* node = GetPage(pager, page_num);
  uint32_t num_keys, child;

  switch (GetNodeType(node)) {
    case (NODE_LEAF):
      num_keys = *LeafNodeNumCells(node);
      Indent(indentation_level);
      printf("- leaf (size %d)\n", num_keys);
      for (uint32_t i = 0; i < num_keys; i++) {
        Indent(indentation_level + 1);
        printf("- %d\n", *LeafNodeKey(node, i));
      }
      break;
    case (NODE_INTERNAL):
      num_keys = *InternalNodeNumKeys(node);
      Indent(indentation_level);
      printf("- internal (size %d)\n", num_keys);
      if (num_keys > 0) {
        for (uint32_t i = 0; i < num_keys; i++) {
          child = *InternalNodeChild(node, i);
          PrintTree(pager, child, indentation_level + 1);

          Indent(indentation_level + 1);
          printf("- key %d\n", *InternalNodeKey(node, i));
        }
        child = *InternalNodeRightChild(node);
        PrintTree(pager, child, indentation_level + 1);
      }
      break;
  }
}


//MAIN FUNCTION
int main(int argc, char* argv[]) {
    PrintPrompt();
    string filename;
    cin >> filename;
    cout << "\n";
    Table* table = DbOpen(filename.c_str());
    InputBuffer* inputBuffer = new InputBuffer();
    while(true) {
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

            case(PREPARE_SPACE):
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
        PrintTree(table->pager, table->rootPageNum, 1);
    }
    
    return 0;
}