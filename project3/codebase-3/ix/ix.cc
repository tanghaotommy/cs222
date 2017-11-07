
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return PagedFileManager::instance()->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return PagedFileManager::instance()->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    RC rc = PagedFileManager::instance()->openFile(fileName, ixfileHandle.fileHandle);
    if (rc == 0)
        ixfileHandle.fileHandle.collectCounterValues(ixfileHandle.ixReadPageCounter, 
            ixfileHandle.ixWritePageCounter, ixfileHandle.ixAppendPageCounter);
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return PagedFileManager::instance()->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    if (ixfileHandle.fileHandle.getNumberOfPages() == 0)
    {
        Node root = Node(attribute);
        root.nodeType = RootOnly;
        root.insertKey(key);
        vector <RID> records;
        records.push_back(rid);
        root.pointers.push_back(records);
        // root.printRids();
        // printf("\n");
        void *page = malloc(PAGE_SIZE);
        root.serialize(page);
        ixfileHandle.fileHandle.appendPage(page);
        free(page);
    } 
    // else
    // {
    //     void *page = malloc(PAGE_SIZE);
    //     ixfileHandle.fileHandle.readPage(0, page);
    //     Node root = Node(attribute, page);
    //     if (root.nodeType == RootOnly)
    //     {
    //         int pos = root.getChildPos(key);
    //         root.insertKey(pos, key); //Insert into vector.
    //         root.insertChild(pos, rid); //Insert into vector.
    //         if (root.pointers.size() > root.order - 1)
    //         {
    //             //split
    //         } else
    //         {
    //             void *page = malloc(PAGE_SIZE);
    //             root.serialize(page);
    //             ixfileHandle.fileHandle.writePage(root.cPage, page);
    //             free(page);
    //         }
    //     } else
    //     {
    //         vector<Node> path;
    //         this->traverseToLeafWithPath(root, path);
    //         Node *leaf = &path[path.size() - 1];

    //         int pos = leaf->getChildPos(key);
    //         leaf->insertKey(pos, key); //Insert into vector.
    //         leaf->insertChild(pos, rid); //Insert into vector.
    //         if (leaf->pointers.size() > leaf->order - 1)
    //         {
    //             //split
    //         }
    //         else {
    //             void *page = malloc(PAGE_SIZE);
    //             leaf->serialize(page);
    //             ixfileHandle.fileHandle.writePage(leaf->cPage, page);
    //             free(page);
    //         }
    //     }
    // }
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    ix_ScanIterator.attribute = &attribute;
    ix_ScanIterator.lowKey = lowKey;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    ix_ScanIterator.ixfileHandle = &ixfileHandle;
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    printNode(ixfileHandle, attribute, 0);
}

void IndexManager::printNode(IXFileHandle &ixfileHandle, const Attribute &attribute, const int &pageNum) const
{
    void *page = malloc(PAGE_SIZE);
    ixfileHandle.fileHandle.readPage(pageNum, page);
    Node node(&attribute, page);
    if (node.nodeType == LeafNode || node.nodeType == RootOnly)
    {
        printf("{\n");
        node.printRids();
        printf("\n}\n");
    }
    else
    {
        printf("{\n");
        printf("\"keys\":[");
        node.printKeys();
        printf("], \n");
        printf("\"children\":[\n");
        for (int i = 0; i < node.children.size(); ++i)
        {
            printNode(ixfileHandle, attribute, node.children[i]);
            if (i != node.children.size() - 1)
                printf(",");
            printf("\n");
        }
        printf("]");
        printf("}\n");
    }
    free(page);
}

IX_ScanIterator::IX_ScanIterator()
{
    this->cPage = 0;
    this->cKey = 0;
    this->cRec = -1;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    if (!this->cPage < this->ixfileHandle->fileHandle.getNumberOfPages())
        return 1;
    void *page = malloc(PAGE_SIZE);
    this->ixfileHandle->fileHandle.readPage(this->cPage, page);
    Node *node = new Node(this->attribute, page);
    node->printRids();
    printf("\n");

    if (node->nodeType != RootOnly && node->nodeType != LeafNode)
    {
        traverseToLeaf(node);
        this->cPage = node->cPage;
    }
    while(1)
    {
        this->cRec++;
        if (this->cRec >= node->pointers[this->cKey].size() - 1)
        {
            this->cRec = 0;
            this->cKey++;
        }
        if (this->cKey >= node->pointers.size() && node->next != -1)
        {
            void *page = malloc(PAGE_SIZE);
            this->ixfileHandle->fileHandle.readPage(node->next, page);
            node = new Node(this->attribute, page);
            this->cRec = 0;
            this->cKey = 0;
        }

        if (this->cRec >= node->pointers.size() && node->next == -1)
        {
            delete node;
            free(page);
            return IX_EOF;
        }
        // else
        // {
        //     this->cRec = 0;
        //     this->cPage = node->next;
        //     this->ixfileHandle->fileHandle.readPage(this->cPage, page);
        //     node = new Node(this->attribute, page);
        // }
        if (lowKey != NULL)
            if (!((this->lowKeyInclusive && isLargerAndEqualThan(this->attribute, node->keys[this->cRec], this->lowKey)) || 
                (!this->lowKeyInclusive && isLargerThan(this->attribute, node->keys[this->cRec], this->lowKey))))
                continue;
        if (highKey != NULL)
            if (!((this->highKeyInclusive && isLessAndEqualThan(this->attribute, node->keys[this->cRec], this->highKey)) || 
                (!this->highKeyInclusive && isLessThan(this->attribute, node->keys[this->cRec], this->highKey))))
            {
                delete node;
                free(page);
                return IX_EOF;        
            }
        rid.pageNum = node->pointers[this->cKey][this->cRec].pageNum;
        rid.slotNum = node->pointers[this->cKey][this->cRec].slotNum;
        if (this->attribute->type == TypeInt || this->attribute->type == TypeReal)
            memcpy(key, node->keys[this->cRec], this->attribute->length);
        else
        {
            int nameLength;
            memcpy(&nameLength, (char *)node->keys[this->cRec], sizeof(int));
            memcpy(key, &nameLength, sizeof(int));
            memcpy((char *)key + sizeof(int), (char *)node->keys[this->cRec] + sizeof(int), nameLength);
        }
        break;
    }
    delete node;
    free(page);
    return 0;
}

RC IX_ScanIterator::traverseToLeaf(Node *node)
{
    while (node->nodeType != RootOnly && node->nodeType != LeafNode)
    {
        int pos = node->getChildPos(this->lowKey);
        void *page = malloc(PAGE_SIZE);
        this->ixfileHandle->fileHandle.readPage(node->children[pos], page);
        node = new Node(this->attribute, page);
        free(page);
    }
    return 0;
}

RC IX_ScanIterator::close()
{
    this->cPage = 0;
    this->cKey = 0;
    this->cRec = -1;
    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
    fileHandle.closeFile();
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    fileHandle.collectCounterValues(this->ixReadPageCounter, this->ixWritePageCounter, this->ixAppendPageCounter);
    readPageCount = this->ixReadPageCounter;
    writePageCount = this->ixWritePageCounter;
    appendPageCount = this->ixAppendPageCounter;
    return 0;
}

Node::Node(const Attribute &attribute)
{
    this->attribute = &attribute;
    this->attrType = attribute.type;
}


Node::Node(const Attribute *attribute, const void *page)
{
    this->isLoaded = true;
    this->attrType = attribute->type;
    this->attribute = attribute;
    int offset = 0;
    memcpy(&this->nodeType, (char *)page + offset, sizeof(NodeType));
    offset += sizeof(NodeType);
    memcpy(&this->previous, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    memcpy(&this->next, (char *)page + offset, sizeof(int));
    offset += sizeof(int);

    int nKeys;
    memcpy(&nKeys, (char *)page + offset, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < nKeys; ++i)
    {
        if (this->attrType == TypeInt)
        {
            void * data = malloc(attribute->length);
            memcpy(data, (char *)page + offset, sizeof(attribute->length));
            offset += sizeof(attribute->length);
            this->keys.push_back(data);
        }
        else if (this->attrType == TypeReal)
        {
            void * data = malloc(attribute->length);
            memcpy(data, (char *)page + offset, sizeof(attribute->length));
            offset += sizeof(attribute->length);
            this->keys.push_back(data);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)page + offset, sizeof(int));
            offset += sizeof(int);
            // printf("String length: %d\n", nameLength);
            void* data = malloc(nameLength + sizeof(int));
            memcpy(data, &nameLength, sizeof(int));
            memcpy((char *) data + sizeof(int), (char *)page + offset, nameLength);
            offset += nameLength;
            this->keys.push_back(data);
        }
    }

    if (this->nodeType == LeafNode || this->nodeType == RootOnly)
    {
        int nRids;
        memcpy(&nRids, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
#ifdef DEBUG_IX
        printf("[Node] nRids: %d, offset: %d\n", nRids, offset - sizeof(int));
#endif
        for (int i = 0; i < nRids; ++i)
        {
            int nRecords;
            memcpy(&nRecords, (char *)page + offset, sizeof(int));
            offset += sizeof(int);
#ifdef DEBUG_IX
            printf("[Node] nRecords %d, offset: %d\n", nRecords, offset - sizeof(int));
#endif
            vector <RID> records;
            for (int j = 0; j < nRecords; ++j)
            {
                RID rid;
                memcpy(&rid.pageNum, (char *)page + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&rid.slotNum, (char *)page + offset, sizeof(int));
                offset += sizeof(int);
                records.push_back(rid);
            }
            this->pointers.push_back(records);
        }
    }
    else
    {
        int nChildren;
        memcpy(&nChildren, (char *)page + offset, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nChildren; ++i)
        {
            int value;
            memcpy(&value, (char *)page + offset, sizeof(int));
            offset += sizeof(int);
            this->children.push_back(value);
        }
    }
}

RC Node::serialize(void * page)
{
    int offset = 0;
    memcpy((char *)page + offset, &this->nodeType, sizeof(NodeType));
    offset += sizeof(NodeType);
    memcpy((char *)page + offset, &this->previous, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)page + offset, &this->next, sizeof(int));
    offset += sizeof(int);

    int nKeys = this->keys.size();
    memcpy((char *)page + offset, &nKeys, sizeof(int));
    offset += sizeof(int);
    for (int i = 0; i < nKeys; ++i)
    {
        if (this->attrType == TypeInt)
        {
            memcpy((char *)page + offset, this->keys[i], sizeof(attribute->length));
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeReal)
        {
            memcpy((char *)page + offset, this->keys[i], sizeof(attribute->length));
            offset += sizeof(attribute->length);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, this->keys[i], sizeof(int));
            memcpy((char *)page + offset, &nameLength, sizeof(int));
            offset += sizeof(int);
            // printf("String length: %d\n", nameLength);
            memcpy((char *)page + offset, (char *) this->keys[i] + sizeof(int), nameLength);
            offset += nameLength;
        }
    }

    if (this->nodeType == LeafNode || this->nodeType == RootOnly)
    {
        int nRids = this->pointers.size();
#ifdef DEBUG_IX
        printf("[serialize] nPointers %d, offset: %d\n", pointers.size(), offset);
#endif
        memcpy((char *)page + offset, &nRids, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nRids; ++i)
        {
            int nRecords = pointers[i].size();
            memcpy((char *)page + offset, &nRecords, sizeof(int));
            offset += sizeof(int);
#ifdef DEBUG_IX
            printf("[serialize] nRecords %d, offset: %d\n", pointers[i].size(), offset - sizeof(int));
#endif
            for (int j = 0; j < nRecords; ++j)
            {
                int pageNum = this->pointers[i][j].pageNum;
                int slotNum = this->pointers[i][j].slotNum;
                memcpy((char *)page + offset, &pageNum, sizeof(int));
                offset += sizeof(int);
                memcpy((char *)page + offset, &slotNum, sizeof(int));
                offset += sizeof(int);
            }
        }
    }
    else
    {
        int nChildren = this->children.size();
        memcpy((char *)page + offset, &nChildren, sizeof(int));
        offset += sizeof(int);
        for (int i = 0; i < nChildren; ++i)
        {
            int value = this->children[i];
            memcpy((char *)page + offset, &value, sizeof(int));
            offset += sizeof(int);
        }
    }
    return 0;
}

RC Node::insertKey(const void* key)
{
    printf(".....inserting keys, type: %d\n", this->attrType);
    if (this->attrType == TypeInt)
    {
        printf(".....inserting keys int\n");
        void * data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
        this->keys.push_back(data);
    }
    else if (this->attrType == TypeReal)
    {
        void * data = malloc(attribute->length);
        memcpy(data, (char *)key, sizeof(attribute->length));
        this->keys.push_back(data);
    }
    else if (this->attrType == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)key, sizeof(int));
        // printf("String length: %d\n", nameLength);
        void* data = malloc(nameLength + sizeof(int));
        memcpy(data, &nameLength, sizeof(int));
        memcpy((char *) data + sizeof(int), (char *)key + sizeof(int), nameLength);
        this->keys.push_back(data);
    }
    return 0;
}

Node::~Node()
{
    for (int i = 0; i < this->keys.size(); ++i)
    {
        free(keys[i]);
    }
}

RC Node::printKeys()
{
    for (int i = 0; i < this->keys.size(); ++i)
    {
        if (this->attrType == TypeInt)
        {
            int value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
            printf("%d", value);
        }
        else if (this->attrType == TypeReal)
        {
            float value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
            printf("%f", value);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)this->keys[i], sizeof(int));
            // printf("String length: %d\n", nameLength);
            char* value_c = (char *)malloc(nameLength + 1);
            memcpy(value_c, (char *)this->keys[i] + sizeof(int), nameLength);
            value_c[nameLength] = '\0';
            printf("\"%s\"\n", value_c);
            free(value_c);
        }

        if (i != this->keys.size() - 1)
            printf(",");
    }
    return 0;
}

RC Node::printRids()
{
    printf("\"keys\":[");
    for (int i = 0; i < this->keys.size(); ++i)
    {
        if (this->attrType == TypeInt)
        {
            int value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
#ifdef DEBUG_IX
            printf("[printRids] nPointers %d\n", pointers.size());
#endif
            printf("%d:", value);
        }
        else if (this->attrType == TypeReal)
        {
            float value;
            memcpy(&value, (char *)this->keys[i], sizeof(attribute->length));
            printf("%f:", value);
        }
        else if (this->attrType == TypeVarChar)
        {
            int nameLength;
            memcpy(&nameLength, (char *)this->keys[i], sizeof(int));
            // printf("String length: %d\n", nameLength);
            char* value_c = (char *)malloc(nameLength + 1);
            memcpy(value_c, (char *)this->keys[i] + sizeof(int), nameLength);
            value_c[nameLength] = '\0';
            printf("\"%s:", value_c);
            free(value_c);
        }

        printf("[");
        for (int j = 0; j < this->pointers[i].size(); ++j)
        {
            printf("(%d, %d)", this->pointers[i][j].pageNum, this->pointers[i][j].slotNum);
            if (i != this->keys.size() - 1)
                printf(",");
        }
        printf("]");
        if (this->attrType == TypeVarChar)
            printf("\"");
        if (i != this->keys.size() - 1)
            printf(",");
    }
    printf("]");
    return 0;
}

int Node::getChildPos(const void* value)
{
    int i;
    for (i = 0; i < this->keys.size(); ++i)
    {
        if (isLessThan(this->attribute, value, this->keys[i]))
            break;
    }
    return i;
}

bool isLessThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value < key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value < key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value < key;
    }    
}

bool isLessAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value <= key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value <= key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value <= key;
    }    
}

bool isLargerAndEqualThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value >= key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value >= key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value >= key;
    }    
}

bool isLargerThan(const Attribute *attribute, const void* compValue, const void* compKey)
{
    if (attribute->type == TypeInt)
    {
        int value;
        int key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value > key;
    }
    else if (attribute->type == TypeReal)
    {
        float value;
        float key;
        memcpy(&value, (char *)compValue, sizeof(attribute->length));
        memcpy(&key, (char *)compKey, sizeof(attribute->length));
        return value > key;
    }
    else if (attribute->type == TypeVarChar)
    {
        int nameLength;
        memcpy(&nameLength, (char *)compValue, sizeof(int));
        char* value_c = (char *)malloc(nameLength + 1);
        memcpy(value_c, (char *)compValue + sizeof(int), nameLength);
        value_c[nameLength] = '\0';
        string value = string(value_c);

        memcpy(&nameLength, (char *)compKey, sizeof(int));
        char* key_c = (char *)malloc(nameLength + 1);
        memcpy(key_c, (char *)compKey + sizeof(int), nameLength);
        key_c[nameLength] = '\0';
        string key = string(key_c);
        free(value_c);
        free(key_c);
        return value > key;
    }    
}